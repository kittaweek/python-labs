import os

from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()


class GCS:
    def __init__(self):
        self.__credentials_file = os.getenv("GCP_CREDENTIALS")
        self.__bucket_name = os.getenv("BUCKET_NAME")
        self.__client = storage.Client.from_service_account_json(
            self.__credentials_file
        )

    def list_object_files(self, symbol: str):
        object_path = f"raws/time_series_{symbol}"
        obj_files = self.__client.list_blobs(self.__bucket_name, prefix=object_path)
        return [obj_file.name for obj_file in obj_files]


if __name__ == "__main__":
    gcs_obj = GCS()
    obj_files = gcs_obj.list_object_files(symbol="usd_jpy")
    print(obj_files)
