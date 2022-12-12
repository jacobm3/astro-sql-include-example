import os
import tempfile
from typing import TYPE_CHECKING, Dict, Optional, Sequence

from airflow.models import BaseOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

if TYPE_CHECKING:
    from airflow.utils.context import Context

class S3ToAzureBlobOperator(BaseOperator):
    """
    Operator transfers data from Azure Blob Storage to specified bucket in Google Cloud Storage
    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AzureBlobStorageToGCSOperator`
    :param wasb_conn_id: Reference to the wasb connection.
    :param load_options: Optional keyword arguments that ``WasbHook.load_file()`` takes.
    :param blob_name: Name of the blob
    :param container_name: Name of the container
    :param aws_conn_id: The connection ID to use when fetching connection info.
    :param s3_bucket: The targeted s3 bucket. This is the S3 bucket from where the file is downloaded.
    :param s3_prefix: The prefix to filter the objects in the S3 bucket.
    :param wasb_overwrite_object: Whether the blob to be uploaded
        should overwrite the current data.
        When wasb_overwrite_object is True, it will overwrite the existing data.
        If set to False, the operation might fail with
        ResourceExistsError in case a blob object already exists.
    :param create_container: Attempt to create the target container prior to uploading the blob. This is
        useful if the target container may not exist yet. Defaults to False.
    """

    def __init__(
        self,
        *,
        wasb_conn_id='wasb_default',
        aws_conn_id: str = "aws_default",
        load_options: Optional[Dict] = None,
        blob_name: str,
        container_name: str,
        s3_bucket: str,
        s3_prefix: Optional[str],
        wasb_overwrite_object: bool = False,
        create_container: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.wasb_conn_id = wasb_conn_id
        self.aws_conn_id = aws_conn_id
        self.load_options = load_options or {"overwrite": wasb_overwrite_object}
        self.create_container = create_container
        self.blob_name = blob_name
        self.container_name = container_name
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix

    template_fields: Sequence[str] = (
        "blob_name",
        "container_name",
        "s3_bucket",
        "s3_prefix"
    )

    def execute(self, context: "Context") -> str:
        azure_hook = WasbHook(wasb_conn_id=self.wasb_conn_id)
        s3_hook = S3Hook(self.aws_conn_id)
        s3_client = s3_hook.get_conn()
        
        with tempfile.NamedTemporaryFile() as temp_file:
            self.log.info(f"Looking for files in {self.s3_bucket} with prefix {self.s3_prefix}")
            
            s3_files = s3_hook.list_keys(bucket_name=self.s3_bucket, prefix=self.s3_prefix, delimiter='_')
            if s3_files:
                s3_key = s3_hook.list_keys(bucket_name=self.s3_bucket, prefix=self.s3_prefix, delimiter='_')[0]
            else:
                raise ValueError(f"No files found for prefix {self.s3_prefix} in bucket {self.s3_bucket}")
                
            self.log.info("Downloading data from s3: s3://%s/%s", self.s3_bucket, s3_key)
            
            s3_client.download_file(self.s3_bucket, s3_key, temp_file.name)

            self.log.info(
                "Uploading data from s3 : s3://%s/%s into Azure Blob bucket: wasb://%s/%s", self.s3_bucket, s3_key, self.blob_name, self.container_name
            )

            azure_hook.load_file(
                file_path=temp_file.name,
                container_name=self.container_name,
                blob_name=self.blob_name,
                create_container=self.create_container,
                **self.load_options,
            )

            self.log.info(
                "Resources have been uploaded from s3: %s to Azure Blob:%s",
                self.s3_bucket,
                self.blob_name,
            )

        return f"wasb://{self.blob_name}/{self.container_name}"