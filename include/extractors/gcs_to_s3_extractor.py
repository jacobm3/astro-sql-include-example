import logging
from typing import Optional, List, Dict, Iterator

from openlineage.airflow.extractors.base import TaskMetadata
from openlineage.airflow.extractors.base import BaseExtractor
from openlineage.client.run import Dataset as IODataset
from openlineage.client.facet import BaseFacet, SqlJobFacet, DataSourceDatasetFacet

log = logging.getLogger(__name__)

class GCSToS3OperatorExtractor(BaseExtractor):

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['GCSToS3Operator']

    def extract_on_complete(self, task_instance):

        print("prefix " + self.operator.prefix)
        print("s3_key" + self.operator.dest_s3_key)
        inputs = IODataset(
            namespace="gcs://{}".format(self.operator.bucket),
            # name=self.operator.prefix.replace('/','_'),
            name=self.operator.prefix,
            facets={
                "dataSource": DataSourceDatasetFacet(name="gcs", uri="gcs://{}".format(self.operator.bucket)),
                "file": self.operator.prefix
            }
        )

        outputs = IODataset(
            namespace="s3://{}".format(self.operator.dest_s3_key.split('/')[2]),
            # name='_'.join(self.operator.dest_s3_key.split('/')[3:]) + '_' + self.operator.prefix.replace('/','_'),
            name='/'.join(self.operator.dest_s3_key.split('/')[3:]) + '/' + self.operator.prefix,
            facets={
                "dataSource": DataSourceDatasetFacet(name="s3", uri="s3://{}".format(self.operator.dest_s3_key.split('/')[2])),
                "file": self.operator.dest_s3_key
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