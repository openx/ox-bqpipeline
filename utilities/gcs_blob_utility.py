import argparse
import datetime

from google.cloud import storage


class GCSClient():
    """GCS Python Client Wrapper.

    Provides methods for listing all objects within a bucket and generating
    a signed URL for given (list of) object(s).
    """
    def __init__(self, json_credentials_path=None):
        self.json_credentials_path = json_credentials_path
        self.project = None # inferred from service account.
        self.gcs = None

    def get_client(self):
        """
        Initializes bigquery.Client
        :return bigquery.Client
        """
        if self.gcs is None:
            if self.json_credentials_path is not None:
                self.gcs = storage.Client.from_service_account_json(
                    project=self.project,
                    json_credentials_path=self.json_credentials_path)
            else:
                self.gcs = storage.Client(project=self.project)

            self.project = self.gcs.project
        return self.gcs

    def generate_signed_urls(self, bucket_name, credentials, expiration=None):
        """Returns a list of signed URLs for given GCS bucket.

        Given a bucket name and service credentials, returns a list of signed URLs
        for all objects within the bucket.
        """
        blobs, prefixes = self.list_blobs_with_prefix(bucket_name)

        if expiration.endswith('m'):
            expiry = datetime.timedelta(minutes=15)
        elif expiration.endswith('s'):
            expiry = datetime.timedelta(seconds=15)
        elif expiration.endswith('h'):
            expiry = datetime.timedelta(hours=15)
        else:
            expiry = datetime.strptime(expiration, '%Y-%m-%d %H:%M:%S')
        urls = []
        for blob in blobs:
            urls.append(blob.generate_signed_url(version='v4',
                                                 expiration=expiry,
                                                 method='GET'))
        for prefix in prefixes:
            urls.append(blob.generate_signed_url(version='v4',
                                                 expiration=expiry,
                                                 method='GET'))

        return urls

    def list_blobs_with_prefix(self, bucket_name, prefix, delimiter=None):
        """Lists all the blobs in the bucket that begin with the prefix.

        This can be used to list all blobs in a "folder", e.g. "public/".

        The delimiter argument can be used to restrict the results to only the
        "files" in the given "folder". Without the delimiter, the entire tree under
        the prefix is returned. For example, given these blobs:
        """

        # Note: Client.list_blobs requires at least package version 1.17.0.
        self.gcs = self.get_client()
        blobs = self.gcs.list_blobs(bucket_name, prefix=prefix,
                                          delimiter=delimiter)

        blob_names = []
        for blob in blobs:
            blob_names.append(blob.name)

        prefixes = []
        if delimiter:
            for prefix in blobs.prefixes:
                prefixes.append(prefix)
        return blob_names, prefixes


    def generate_download_signed_url_v4(self, bucket_name, blob_name):
        """Generates a v4 signed URL for downloading a blob.

        Note that this method requires a service account key file. You can not use
        this if you are using Application Default Credentials from Google Compute
        Engine or from the Google Cloud SDK.
        """
        self.gcs = self.get_client()
        bucket = self.gcs.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)

        url = blob.generate_signed_url(
            version='v4',
            # This URL is valid for 15 minutes
            expiration=datetime.timedelta(minutes=15),
            # Allow GET requests using this URL.
            method='GET')

        print('Generated GET signed URL:')
        print(url)
        print('You can use this URL with any user agent, for example:')
        print('curl \'{}\''.format(url))
        return url


def main():
    """
    Handles CLI invocations of bqpipelines.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--secret_key_file', dest='secret_key_file', required=True,
                        help="Path to your service account secret key.")
    parser.add_argument('--project', dest='project', required=False,
                        help="GCP project id.", default=None)
    parser.add_argument('--bucket_name', dest='bucket_name', required=False,
                        help="Bucket name that we are generating signed urls for.", default='CSV')
    parser.add_argument('--prefix', dest='prefix', required=False,
                        help="Prefix within the given bucket to lookup.", default='CSV')
    parser.add_argument('--delimiter', dest='delimiter', required=False,
                        help="Delimiter.", default=None)
    args = parser.parse_args()

    gcs = GCSClient(json_credentials_path=args.secret_key_file)
    blobs, prefixes = gcs.list_blobs_with_prefix(args.bucket_name, prefix=args.prefix,
                                                 delimiter=args.delimiter)
    if blobs:
        print(gcs.generate_download_signed_url_v4(args.bucket_name, blobs[0]))

if __name__ == "__main__":
    main()
