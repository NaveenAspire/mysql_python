
"""This module has s3 service connection and uploading file into s3"""
import boto3
from botocore.exceptions import ClientError


class S3Service:
    """This class has the methods for s3 service"""
    def __init__(self):
        """This is the init method of the class S3Service"""
        self.s3_obj = boto3.client("s3",
            aws_access_key_id="AKIAYJBSUNZ3TOTVTE63",
            aws_secret_access_key="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")

    def upload_file_to_s3(self, file, bucket_name, key):
        """This function done the file uploading in s3 aspire-data-dev bucket"""
        status = ""
        try:
            self.s3_obj.upload_file(file, bucket_name, key)
            status = "Updated Sucessfully"
        except ClientError as error:
            print(error)

