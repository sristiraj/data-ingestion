def dwnld(sftp_client, remote_file, local_file, callback):
    file_size = sftp_client.stat(remote_file).st_size
    chunk_size = 0x8000
    offset = 0
    chunks = []

    print("Remote file size: {}".format(file_size))

    while offset < file_size:
        remaining  = file_size - offset
        if remaining < chunk_size:
            chunks.append((offset,remaining))
        else:    
            chunks.append((offset,chunk_size))
        offset += chunk_size

    with sftp_client.open(remote_file) as fl:
        received_data = b''
        received_data_chunks_gen = fl.readv(chunks)
        for block in received_data_chunks_gen:
            received_data += block
            
    with open(local_file,"wb") as f:
        f.write(block)    
