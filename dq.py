
from pyspark.sql import SparkSession


SINGLE_DATASET_RULE_TYPES = ["count_check"]
MULTIPLE_DATASET_RULE_TYPES = []

def get_rule_group(rule_type):
    if rule_type in SINGLE_DATASET_RULE_TYPES:
        rule_group = "SINGLE"
    elif rule_type in MULTIPLE_DATASET_RULE_TYPES:
        rule_group = "MULTIPLE"
    else:
        rule_group = "UNKNOWN"
    return rule_group            

def read_oracle(dataset):
    pass

def read_s3(dataset):
    pass

def read_sybase(dataset):
    pass

def read_snowflake(dataset):
    pass

def read_dataset(dataset):
    if dataset["table_type"].lower() == "oracle":
        df = read_oracle(dataset)
    elif  dataset["table_type"].lower() == "s3":
        df = read_s3(dataset)
    elif dataset["table_type"].lower() == "sybase":
        df = read_sybase(dataset)

def write_rule_metrics_to_repo(metrics, metrics_storage):
    pass

def run_rule_single(df1 , rule_type):
    pass

def run_rule_multiple(df1, df2, rule_type):
    pass

if __name__=="__main__":

    args = {}

    rule_name = args["rule_name"]
    rule_types = args["rule_types"]

    dataset1 = None
    dataset2 = None
    
    for rule_type in rule_types:
        rule_group = get_rule_group(rule_type)
    
        if rule_group == "SINGLE":
            dataset1 = args["dataset1"]
        elif rule_group == "MULTIPLE":
            dataset1 = args["dataset1"]
            dataset2 = args["dataset2"]    
        else:
            print("invalid rule type. Please configure rule type from the options: {} for single dataset dataquality and {} for 2 dataset dataquality".format(",".join(SINGLE_DATASET_RULE_TYPES), ",".join(MULTIPLE_DATASET_RULE_TYPES)))
            raise Exception("invalid rule type. Please configure rule type from the options: {} for single dataset dataquality and {} for 2 dataset dataquality".format(",".join(SINGLE_DATASET_RULE_TYPES), ",".join(MULTIPLE_DATASET_RULE_TYPES)))
        
        if rule_group == "SINGLE":
            df1 = read_dataset(dataset1)
        elif rule_group == "MULTIPLE":
            df1 = read_dataset(dataset1)
            df2 = read_dataset(dataset2)

        result = ""
        if rule_group == "SINGLE":
            result = run_rule_single(df1, rule_type)
        elif rule_group == "MULTIPLE":
            result = run_rule_multiple(df1, df2, rule_type)

        write_rule_metrics_to_repo(result)
