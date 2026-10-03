import sys
sys.path.append("Airflowpipelines")
from airflow.models import DAG, BaseOperator, TaskInstance
from hook.twitter_hook import TwitterHook
import json
from datetime import datetime, timedelta
from os.path import join
from pathlib import Path

class TwitterOperator(BaseOperator):
    
    template_fields = ["file_path", "start_time", "end_time", "query"]
    
    def __init__(self, file_path, end_time, start_time, query, **kwargs):
        self.file_path = file_path
        self.end_time = end_time
        self.start_time = start_time
        self.query = query
        
        super().__init__(**kwargs)

    def create_parent_folder(self):
        # Create the parent folder if it doesn't exist
        parent_folder = Path(self.file_path).parent
        parent_folder.mkdir(parents=True, exist_ok=True)

    def execute(self, context):
        end_time = self.end_time
        start_time = self.start_time
        query = self.query
        
        self.create_parent_folder()
        
        with open(self.file_path, 'w') as output_file:
            for pg in TwitterHook(end_time, start_time, query).run():
                json.dump(pg, output_file, ensure_ascii=False)
                output_file.write('\n')
                
if __name__ == "__main__":
    TIMESTAMP_FORMAT = f"%Y-%m-%dT%H:%M:%S.00Z"

    end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
    start_time = (datetime.now() + timedelta(days=-1)).date().strftime(TIMESTAMP_FORMAT)
    query = "datascience"
    
    with DAG(dag_id="TwitterTest", start_date=datetime.now()):
        to = TwitterOperator(file_path=join("datalake/twitter_datascience",
                            f"extract_date={datetime.now().date().strftime('%Y-%m-%d')}",
                            f"datascience_{datetime.now().date().strftime('%Y-%m-%d')}.json"),
                            query=query, 
                            start_time=start_time, 
                            end_time=end_time, 
                            task_id="test_run")
        ti = TaskInstance(task=to, dag_version_id=1)
        to.execute(ti.task_id)