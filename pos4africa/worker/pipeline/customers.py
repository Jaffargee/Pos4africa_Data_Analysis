from pos4africa.worker.pipeline.base import BaseComponent, PipelineContext
from pos4africa.worker.components.extractors.customer_extractor import CustomerExtractor
from pos4africa.worker.components.syncer import Syncer
from pos4africa.manager.egress.batch_writer import BatchWriter
from pos4africa.config import settings

class CustomerSyncer(BaseComponent):
      async def run(self, ctx: PipelineContext) -> None:

            writer = BatchWriter()
            syncer = Syncer()
            extractor = CustomerExtractor(ctx.node_id, ctx.memory)

            customers = await extractor.run(excel_path=settings.customer_excel_path, sheet_name=settings.customer_sheet_name)
            customers = syncer.sync(customers)

            inserted_customers = await writer.write_customers(syncer.normalize_customer_data(customers))
            syncer.save_json_object(syncer.customers_local_db_file_path, syncer.get_customers())

            ctx.metrics["inserted_customers"] = inserted_customers

            
