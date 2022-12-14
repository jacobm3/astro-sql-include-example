import logging
from typing import Optional, List, Dict, Iterator
from contextlib import closing

from openlineage.airflow.extractors.base import TaskMetadata
from openlineage.airflow.extractors.snowflake_extractor import SnowflakeExtractor
from openlineage.common.dataset import Source, Dataset
from openlineage.client.run import Dataset as InputDataset
from openlineage.client.facet import SqlJobFacet
from openlineage.common.models import DbTableSchema, DbColumn
from openlineage.common.sql import DbTableMeta
from airflow.models import Connection
# from openlineage.airflow.extractors.dbapi_utils import execute_query_on_hook

log = logging.getLogger(__name__)

class S3ToSnowflakeOperatorExtractor(SnowflakeExtractor):

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['S3ToSnowflakeOperator']

    def _get_hook(self):
        return self._get_db_hook()

    def _get_db_hook(self):
        """
        Create and return SnowflakeHook.
        :return: a SnowflakeHook instance.
        :rtype: SnowflakeHook
        """
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        return SnowflakeHook(
            snowflake_conn_id=self.operator.snowflake_conn_id,
            warehouse=self.operator.warehouse,
            database=self.operator.database,
            role=self.operator.role,
            schema=self.operator.schema,
            authenticator=self.operator.authenticator,
            session_parameters=self.operator.session_parameters,
        )

    def execute_query_on_hook(self, 
        hook, query) -> Iterator[tuple]:
        with closing(hook.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                return cursor.execute(query).fetchall()

    def _get_table(self, table: str) -> Optional[DbTableSchema]:
        self.log.debug("Getting table details")
        sql = self._information_schema_query([table])
        self.log.debug("Executing query for schema: {}".format(sql))
        fields = self.execute_query_on_hook(hook=self._get_db_hook(), query=sql)
        self.log.debug(type(fields))
        self.log.debug("Table Structure: {}".format(fields))

        if not fields:
            return None

        first_elem = fields[0]
        schema_name = fields[0][0]
        self.log.debug("Database Schema is {}".format(schema_name))

        columns = [
            DbColumn(
                name=fields[i][2],
                type=fields[i][4],
                ordinal_position=i,
            )
            for i in range(len(fields))
        ]

        return DbTableSchema(
            schema_name=schema_name,
            table_name=table,
            columns=columns,
        )

    def _get_table_schemas(self, tables: List[DbTableMeta]) -> List[DbTableSchema]:
        if not tables:
            return []
        return [self._get_table(table) for table in tables]


    def extract_on_complete(self, task_instance) -> Optional[TaskMetadata]:
        inputs = InputDataset(
            namespace="snowflake_stage_{}".format(self.operator.stage), ## since there is no parameter for s3 bucket, stage will have details about the integration. so using this
            # name="incoming/covid19/{}".format(self.operator.s3_keys[0]),
            name=self.operator.s3_keys[0].replace("/", "_"),
            # name=",".join(self.operator.s3_keys) if self.operator.s3_keys else self.operator.prefix,
            facets={}
        )

        source = Source(
            scheme=self._get_scheme(),
            authority=self._get_authority(),
            connection_url=self.get_connection_uri(self.conn)
        )

        database = self.operator.database
        if not database:
            database = self._get_database()
        
        out_table: DbTableMeta = DbTableMeta('.'.join([self.operator.schema, self.operator.table]).upper())
        outputs = [
            Dataset.from_table_schema(
                source=source,
                table_schema=out_table_schema,
                database_name=database
            ) for out_table_schema in self._get_table_schemas(
                [out_table]
            )
        ]

        task_name = f"{self.operator.dag_id}.{self.operator.task_id}"
        run_facets: Dict = {}
        job_facets: Dict = {
            "sql": SqlJobFacet(query=f"""
                        copy into {self.operator.table} 
                        from {self.operator.stage} 
                        files=('{self.operator.s3_keys}') 
                        fileformat=({self.operator.file_format})
                    """)
        }
        
        db_specific_run_facets = self._get_db_specific_run_facets(
            source, inputs, outputs
        )

        run_facets = {**db_specific_run_facets}

        return TaskMetadata( 
            name=task_name,
            inputs=[inputs],
            outputs=[ds.to_openlineage_dataset() for ds in outputs],
            run_facets=run_facets,
            job_facets=job_facets
        )

    def extract(self) -> Optional[TaskMetadata]:
        pass
