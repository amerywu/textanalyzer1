import tools.model.model_classes as merm_model
import tools.utils.es_connect as es_conn
import tools.utils.log as log

index_suffix = "@part_of_doc"








def run_post_process(package: merm_model.PipelinePackage):
    es = es_conn.connectToES()
    if "confluence" in package.any_analysis_dict["provider"]:
        for linked_doc in package.linked_document_list:
            if "page_views" in linked_doc.scores.keys():
                page_view_count = linked_doc.scores["page_views"]
                log.getLogger().info("Updating " + str(linked_doc.ui) + " page_views: " + str(page_view_count))

                es.update(index=linked_doc.index_name, doc_type="_doc", id=linked_doc.uid,body={"doc": {"page_views": page_view_count}})







