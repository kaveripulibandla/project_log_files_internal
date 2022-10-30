import boto3
import env

class S3Session:
	def get_boto3_session():
		#Creating Session With Boto3.
		return boto3.Session(
		aws_access_key_id=env.aws_access_key_id.lstrip("**"),
		aws_secret_access_key=env.aws_secret_access_key.lstrip("**")
		)