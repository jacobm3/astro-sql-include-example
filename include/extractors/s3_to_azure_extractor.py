import logging
from typing import Optional, List, Dict, Iterator

from openlineage.airflow.extractors.base import TaskMetadata
from openlineage.airflow.extractors.base import BaseExtractor
from openlineage.client.run import Dataset as IODataset
from openlineage.client.facet import BaseFacet, SqlJobFacet, DataSourceDatasetFacet

log = logging.getLogger(__name__)

class S3ToAzureBlobOperatorExtractor(BaseExtractor):

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['S3ToAzureBlobOperator']

    def extract_on_complete(self, task_instance):
        print(self.operator.blob_name)
        print(self.operator.container_name)
        print(self.operator.s3_bucket)
        print(self.operator.s3_prefix)
        inputs = IODataset(
            namespace="s3://{}".format(self.operator.s3_bucket),
            name=self.operator.s3_prefix,
            facets={
                "dataSource": DataSourceDatasetFacet(name="s3", uri="s3://{}".format(self.operator.s3_bucket)),
                "file": self.operator.s3_prefix
            }
        )

        outputs = IODataset(
            namespace="wasb://{}".format(self.operator.container_name),
            name=self.operator.blob_name,
            facets={
                "dataSource": DataSourceDatasetFacet(name="wasb", uri="wasb://{}".format(self.operator.container_name)),
                "file": self.operator.blob_name
            }
        )

        task_name = f"{self.operator.dag_id}.{self.operator.task_id}"
        run_facets: Dict = {}
        job_facets: Dict = {}

        return TaskMetadata( 
            name=task_name,
            inputs=[inputs],
            outputs=[outputs],
            run_facets=run_facets,
            job_facets=job_facets
        )

    def extract(self) -> Optional[TaskMetadata]:
        pass