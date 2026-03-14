import boto3
from io import BytesIO, TextIOWrapper
import gzip

def main():
    key = 'crawl-data/CC-MAIN-2022-05/wet.paths.gz'
    bucket = 'commoncrawl'
    s3 = boto3.client('s3',region_name='us-east-1')

    memory_file = BytesIO() # create a ByteIO object
    memory_file.write(s3.get_object(Bucket=bucket,Key=key)['Body'].read()) #write data (in bytes)
    memory_file.seek(0)
    with gzip.open(memory_file,'rb') as f:
        url_of_first_line = f.readline().strip().decode('utf-8')

    stream_body = s3.get_object(Bucket=bucket,Key=url_of_first_line)['Body']

    gzipped_file = gzip.GzipFile(None,'rb',fileobj=stream_body)
    wrapper = TextIOWrapper(gzipped_file)

    for i,line in enumerate(wrapper):
        print(line.strip())
        if i == 9: # stop after 10 lines
            break

if __name__ == "__main__":
    main()
