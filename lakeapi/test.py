# import boto3
# from datetime import datetime, timedelta

# cloudfront_key_pair_id = 'YOUR_CLOUDFRONT_KEY_PAIR_ID'
# cloudfront_private_key_path = 'PATH_TO_YOUR_CLOUDFRONT_PRIVATE_KEY'

# def generate_signed_url(cloudfront_url):
#     cloudfront_signer = boto3.client('cloudfront', region_name='us-east-1')
#     expires = datetime.now() + timedelta(minutes=5) # URL will expire in 5 minutes
#     cloudfront_url = cloudfront_signer.generate_presigned_url(
#         url=cloudfront_url,
#         expires_in=300,
#         key_pair_id=cloudfront_key_pair_id,
#         private_key=open(cloudfront_private_key_path).read(),
#     )
#     return cloudfront_url



import boto3
# import requests

# # Set up the CloudFront signed URL
# cloudfront_url = 'http://data.crypto-lake.com/book/exchange=BINANCE_FUTURES/symbol=BTC-USDT-PERP/dt=2023-09-13/1.snappy.parquet'
# cloudfront_signer = boto3.client('cloudfront', region_name='eu-west-1')
# signed_url = cloudfront_signer.generate_presigned_url(
#     cloudfront_url,
# )

# # Set up the S3 request
# # s3_url = 'YOUR_S3_URL'
# # s3_headers = {'Authorization': 'AWS YOUR_ACCESS_KEY_ID:YOUR_SECRET_ACCESS_KEY'}
# s3_response = requests.get(signed_url)

# # Print the S3 response
# print(s3_response.content)

s3 = boto3.resource('s3')
cf = boto3.client('cloudfront')


video_url = s3.generate_url(
    60,
    'GET',
    bucket = 'qnt.data',
    key = 'market-data/cryptofeed/book/exchange=BINANCE_FUTURES/symbol=BTC-USDT-PERP/dt=2023-09-13/1.snappy.parquet',
    force_http = True
)

origin = cf.origin.S3Origin( "{}.s3.amazonaws.com".format( settings.CONFIG.AWS_VIDEO_STORAGE_BUCKET_NAME ) )
cloudf = boto.connect_cloudfront( 'AKIA3URKA4L4U4Q2C4V5', 'cJHv5yRcNQ9QIsDGUWnWuLdncX3uQ9HGs+Ul7j8P' )
stream_distributions = cloudf.get_all_streaming_distributions()
print(stream_distributions)

# if not len(stream_distributions):
#     distro = cloudf.create_streaming_distribution( origin = origin, enabled = True, comment = 'Video streaming distribution' )
# else:
#     distro = stream_distributions[0]

# return distro.create_signed_url(
#     video_url,
#     expire_time = int( time.time() + 3000 ),
#     private_key_file = self.premium_video_url
# )
