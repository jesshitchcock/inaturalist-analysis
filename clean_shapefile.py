import configparser
import re
import boto3
import geopandas as gpd
from unidecode import unidecode
import os
import awswrangler as wr


def remove_non_ascii(text):
    """This function attempts to remove non ASCII characters from text.
   If it cannot do so, for example for missing values, it returns the original text input."""
    try:
        return unidecode(unicode(text, encoding="utf-8"))
    except:
        return unidecode(str(text))


def clean_shapefile(shapefile_name, s3_bucket_input, s3_bucket_output):
    """This function takes an S3 bucket location and the name of a shapefile (without the .shp extension).
   This function does the following:
   1. Identifies the columns of the shapefile that are objects and could possibly contain non-ASCII characters.
   2. Iterates through this list of columns and
      - Uses the unidecode package to replace non-ASCII characters with the most representative ASCII-compatible character.
      - Removes carriage return and line feed characters from text fields.
   4. Uploads the cleaned file to s3.
   """
    # Read in the config file
    config = configparser.ConfigParser()
    path = os.path.join(os.path.expanduser('~'), '.aws/credentials')
    config.read(path)

    # Load the credentials into the environment variables
    os.environ['AWS_ACCESS_KEY_ID'] = config['default']['aws_access_key_id']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['default']['aws_secret_access_key']
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")

#    s3_client = boto3.client(
#        "s3",
#        aws_access_key_id=AWS_ACCESS_KEY_ID,
#        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
#    )

    s3_shapefile = f'{s3_bucket_input}{shapefile_name}.shp'
    """This function takes shapefile from s3, cleans the columns and writes back to a cleaned file. 
   Params: 's3://inaturalist-bucket/AMPHIBIANS/AMPHIBIANS.shp'"""
    # Step 1: Read the shapefile from an S3 bucket
    df = gpd.read_file(s3_shapefile)

    # Step 2: Identify shapefile columns that are of type object.
    # Create a list of these columns
    string_columns = df.columns[df.dtypes == 'object'].to_list()

    # Step 3: instantiate empty non_ascii_columns list
    non_ascii_columns = []

    # Step 4: Identify the columns with non-ascii characters and append these column names to the non_ascii_columns list
    for col in string_columns:
        # print(f' Checking {col} for non-ASCII characters')
        records = df[col].unique().tolist()
        for record in records:
            if re.search("[^\x1F-\x7F]+", str(record)) is not None:
                if col not in non_ascii_columns:
                    non_ascii_columns.append(col)
                # print(re.findall("[^\x1F-\x7F]+", str(record)))

    # Step 5: Iterate through the columns with non-ascii values and replace these values with unidecoder
    for col in non_ascii_columns:
        df[col] = df.loc[:, col].apply(remove_non_ascii)
        df[col] = df[col].replace(r'\r\n', '', regex=True)
        df[col] = df[col].replace(r'\n', '', regex=True)

    # Step 6: Infer the schema of the shapefile
    df_schema = gpd.io.file.infer_schema(df)

    # Step 7: Write file to current directory
    df.to_file(f"{shapefile_name}_cleaned.shp", schema=df_schema)

    # Step 8: Upload to S3
    #   wr.s3.upload(local_file=f'{shapefile_name}_cleaned.shp', path=s3_bucket_output)


def main():
    """
       Reads shapefile from S3, and replaces non-ASCII characters as well as line feeds and carriage returns.
       Exports the cleaned shapefile back to the same s3 bucket."
       """
    shp_file = "AMPHIBIANS"
    s3_input = "s3://inaturalist-bucket/AMPHIBIANS/"
    s3_output = "s3://inaturalist-bucket/"

    clean_shapefile(shp_file, s3_input, s3_output)


if __name__ == "__main__":
    main()
