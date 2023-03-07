
import logging
import os
import typing
from os.path import join, dirname
import sys
from datetime import datetime
from paramiko import SFTPClient, SFTPFile, Message, SFTPError, Transport
from paramiko.sftp import CMD_STATUS, CMD_READ, CMD_DATA
from stat import S_ISDIR, S_ISREG
# from awsglue.utils import getResolvedOptions
import boto3
from concurrent import futures
from pyspark.sql import SparkSession
import pandas as pd
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas

logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")

args = {"JOB_NAME":"SFTP_SPARK","REGION":"us-east-1", "SFTP_HOST":"localhost", "SFTP_USER":"wicked", "SFTP_PORT":"22", "SFTP_PASS":"troy","SFTP_INPUT_PATH":"/home/wicked/Downloads/shipment","SNOW_USER":"RAJSRISTISNOWFLAKE", "SNOW_PASSWORD":"gOOgle@2340178", "SNOW_ACCOUNT": "NCLUGRS-OA64655", "SNOW_ROLE":"SYSADMIN", "SNOWFLAKE_WH":"COMPUTE_WH", "SNOWFLAKE_DB":"sample_db", "SNOWFLAKE_SCHEMA":"public", "SNOWFLAKE_TABLE":"shipment", "CHECKPOINT_LOC":"_checkpoint/"}

sfparams = {
  "sfURL" : "{}.snowflakecomputing.com".format(args["SNOW_ACCOUNT"]),
  "sfUser" : args["SNOW_USER"],
  "sfPassword" : args["SNOW_PASSWORD"],
  "sfDatabase" : args["SNOWFLAKE_DB"],
  "sfSchema" : args["SNOWFLAKE_SCHEMA"],
  "sfWarehouse" : args["SNOWFLAKE_WH"]
}

# spark = SparkSession.builder.appName("sftp").master("local[*]").getOrCreate()
try:
    snow_conn = connect(
            user=args["SNOW_USER"],
            password=args["SNOW_PASSWORD"],
            account=args["SNOW_ACCOUNT"],
            role=args["SNOW_ROLE"],
            database=args["SNOWFLAKE_DB"]
        )

    print("Connection to snowflake successful")
except:
    print("Connection to snowflake failed")    

spark = SparkSession.builder.appName("sample").config("spark.jars.packages","net.snowflake:spark-snowflake_2.12:2.11.1-spark_3.3").master("local[*]").getOrCreate()


class _SFTPFileDownloader:
    """
    Helper class to download large file with paramiko sftp client with limited number of concurrent requests.
    """

    _DOWNLOAD_MAX_REQUESTS = 48
    _DOWNLOAD_MAX_CHUNK_SIZE = 0x8000

    def __init__(self, f_in: SFTPFile, f_out: typing.BinaryIO, callback=None):
        self.f_in = f_in
        self.f_out = f_out
        self.callback = callback

        self.requested_chunks = {}
        self.received_chunks = {}
        self.saved_exception = None

    def download(self):
        file_size = self.f_in.stat().st_size
        requested_size = 0
        received_size = 0

        while True:
            # send read requests
            while len(self.requested_chunks) + len(self.received_chunks) < self._DOWNLOAD_MAX_REQUESTS and \
                    requested_size < file_size:
                chunk_size = min(self._DOWNLOAD_MAX_CHUNK_SIZE, file_size - requested_size)
                request_id = self._sftp_async_read_request(
                    fileobj=self,
                    file_handle=self.f_in.handle,
                    offset=requested_size,
                    size=chunk_size
                )
                self.requested_chunks[request_id] = (requested_size, chunk_size)
                requested_size += chunk_size

            # receive blocks if they are available
            # note: the _async_response is invoked
            self.f_in.sftp._read_response()
            self._check_exception()

            # write received data to output stream
            while True:
                chunk = self.received_chunks.pop(received_size, None)
                if chunk is None:
                    break
                _, chunk_size, chunk_data = chunk
                self.f_out.write(chunk_data)
                if self.callback is not None:
                    self.callback(chunk_data)

                received_size += chunk_size

            # check transfer status
            if received_size >= file_size:
                break

            # check chunks queues
            if not self.requested_chunks and len(self.received_chunks) >= self._DOWNLOAD_MAX_REQUESTS:
                raise ValueError("SFTP communication error. The queue with requested file chunks is empty and"
                                 "the received chunks queue is full and cannot be consumed.")

        return received_size

    def _sftp_async_read_request(self, fileobj, file_handle, offset, size):
        sftp_client = self.f_in.sftp

        with sftp_client._lock:
            num = sftp_client.request_number

            msg = Message()
            msg.add_int(num)
            msg.add_string(file_handle)
            msg.add_int64(offset)
            msg.add_int(size)

            sftp_client._expecting[num] = fileobj
            sftp_client.request_number += 1

        sftp_client._send_packet(CMD_READ, msg)
        return num

    def _async_response(self, t, msg, num):
        if t == CMD_STATUS:
            # save exception and re-raise it on next file operation
            try:
                self.f_in.sftp._convert_status(msg)
            except Exception as e:
                self.saved_exception = e
            return
        if t != CMD_DATA:
            raise SFTPError("Expected data")
        data = msg.get_string()

        chunk_data = self.requested_chunks.pop(num, None)
        if chunk_data is None:
            return

        # save chunk
        offset, size = chunk_data

        if size != len(data):
            raise SFTPError(f"Invalid data block size. Expected {size} bytes, but it has {len(data)} size")
        self.received_chunks[offset] = (offset, size, data)

    def _check_exception(self):
        """if there's a saved exception, raise & clear it"""
        if self.saved_exception is not None:
            x = self.saved_exception
            self.saved_exception = None
            raise x


def download_file(sftp_client: SFTPClient, remote_path: str, local_path: str, callback=None):
    """
    It contains a fix for a bug that prevents a large file downloading with :meth:`paramiko.SFTPClient.get`
    Note: this function relies on some private paramiko API and has been tested with paramiko 2.7.1.
          So it may not work with other paramiko versions.
    :param sftp_client: paramiko sftp client
    :param remote_path: remote file path
    :param s3_path: s3 file path
    :param callback: optional data callback
    """
    remote_file_size = sftp_client.stat(remote_path).st_size

    with sftp_client.open(remote_path, 'rb') as f_in, open(local_path, 'wb') as f_out:
        _SFTPFileDownloader(
            f_in=f_in,
            f_out=f_out,
            callback=callback
        ).download()

def get_processed_files(checkpoint_loc):
    processed_files = []
    try:
        df = spark.read.json(checkpoint_loc)
        processed_files = [x["file_name"] for x in df.collect()]
    except:
        processed_files=[]
    return processed_files

def get_sftp_files_list(sftp_client, sftp_dir, checkpoint_loc):
    filelist = []
    processed_files = get_processed_files(checkpoint_loc)

    for entry in sftp_client.listdir_attr(sftp_dir):
        mode = entry.st_mode
        if S_ISDIR(mode):
            pass
        elif S_ISREG(mode) and (entry.filename[len(entry.filename)-3:]=="csv" or entry.filename[len(entry.filename)-3:]=="CSV") and entry.filename not in processed_files:
            filelist.append((sftp_dir.rstrip("/")+"/",entry.filename))
    return filelist

def snowflake_write(file_path, database, schema, snowflake_table, checkpoint_location):
    logger.info("writing file {} to snowflake".format(file_path))
    df = spark.read.csv(file_path, header=True)
    df.show(5)
    df = spark.createDataFrame(df.rdd, ["delay_kpi","shipment_id","shipper","customer","origin","destination","avg_delay","pay_amt"])

    df.write.format("snowflake").options(**sfparams).option("dbtable", "public.shipment").mode("append").save()

    num_rows = df.count()
    df_checkpoint = spark.createDataFrame([(file_path,num_rows)],["file_name","rows_processed"])
    df_checkpoint.repartition(1).write.format("json").mode("append").save(checkpoint_location)
    # success, num_chunks, num_rows, output = write_pandas(
    #             conn=snow_conn,
    #             df=df,
    #             table_name=snowflake_table,
    #             schema=schema
    #         )
    logger.info("File {} written to snowflake with {} num. of rows written".format(file_path, num_rows))        

def sftp_copy(file_attr):
    user = file_attr["username"]
    passwd = file_attr["password"]
    file_attr.pop("username")
    file_attr.pop("password")
    logger.info("Start SFTP copy to snowflake operation for {}".format(str(file_attr)))

    transport = Transport((file_attr["host"], file_attr["port"]))
    transport.set_keepalive(30)
    transport.connect(
        username=user,
        password=passwd,
    )
    with SFTPClient.from_transport(transport) as sftp_client:
        progress_size = 0
        total_size = 0
        step_size = 4 * 1024 * 1024

        def progress_callback(data):
            nonlocal progress_size, total_size
            progress_size += len(data)
            total_size += len(data)
            while progress_size >= step_size:
                logger.info(f"{total_size // (1024 ** 2)} MB has been downloaded")
                progress_size -= step_size

        download_file(
            sftp_client=sftp_client,
            remote_path=file_attr["remote_dir"]+file_attr["filename"],
            local_path=file_attr["filename"],
            callback=progress_callback
        )

        snowflake_write(file_attr["filename"], file_attr["snowflake_db"], file_attr["snowflake_schema"], file_attr["snowflake_table"], file_attr["checkpoint_location"])

def main():
    # args = getResolvedOptions(sys.argv, ["JOB_NAME","REGION_NAME", "SFTP_HOST", "SFTP_USER", "SFTP_PORT", "SFTP_PASS","SFTP_INPUT_PATH","S3_OUTPUT_PATH"])    #sftp server
    host = args["SFTP_HOST"]
    port = int(args["SFTP_PORT"])
    username = args["SFTP_USER"]
    password = args["SFTP_PASS"]
    remote_file_path = args["SFTP_INPUT_PATH"]
    # local_file_path = join(dirname(__file__), 'tmpfile')
    snowflake_db = args["SNOWFLAKE_DB"]
    snowflake_schema = args["SNOWFLAKE_SCHEMA"]
    snowflake_table = args["SNOWFLAKE_TABLE"]
    checkpoint_location = args["CHECKPOINT_LOC"]


    transport = Transport((host, port))
    transport.set_keepalive(30)
    transport.connect(
        username=username,
        password=password,
    )
    filelist = []
    fileiter = []

    with SFTPClient.from_transport(transport) as sftp_client:
        progress_size = 0
        total_size = 0
        step_size = 4 * 1024 * 1024

        filelist = get_sftp_files_list(sftp_client, remote_file_path, checkpoint_location)

        fileiter = [{"host":host, "port":port, "username": username, "password": password, "remote_dir":f[0], "filename":f[1], "snowflake_db":snowflake_db, "snowflake_schema": snowflake_schema, "snowflake_table": snowflake_table, "checkpoint_location": checkpoint_location} for f in filelist]

    logger.info("Files to be processed {}".format(str([x["filename"] for x in fileiter])))

    with futures.ProcessPoolExecutor(4) as executor:
        for file_attr, result  in zip(fileiter, executor.map(sftp_copy, fileiter)):
            print("File {} processed with result {}".format(file_attr["filename"], result))


try:

    main()
finally:    
    logger.info("Snowflake connection closed")
    snow_conn.close()
    spark.stop()
