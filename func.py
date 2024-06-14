import base64
import io
import json
import logging
import os
import requests
import uuid

import oci.ai_document
import oci.object_storage
import oci.retry

from datetime import datetime
from fdk import response

try:
    signer = oci.auth.signers.get_resource_principals_signer()
    object_storage_client = oci.object_storage.ObjectStorageClient(
        config={}, signer=signer
    )
    ai_document_client = oci.ai_document.AIServiceDocumentClientCompositeOperations(
        oci.ai_document.AIServiceDocumentClient({}, signer=signer)
    )

    if os.getenv("NAMESPACE_NAME") is None:
        raise ValueError("ERROR: Missing configuration key NAMESPACE_NAME")

    if os.getenv("COMPARTMENT_OCID") is None:
        raise ValueError("ERROR: Missing configuration key COMPARTMENT_OCID")

    if os.getenv("BUCKET_NAME_IN") is None:
        raise ValueError("ERROR: Missing configuration key BUCKET_NAME_IN")

    if os.getenv("BUCKET_NAME_OUT") is None:
        raise ValueError("ERROR: Missing configuration key BUCKET_NAME_OUT")

    if os.getenv("DESTINATION_URL") is None:
        raise ValueError("ERROR: Missing configuration key DESTINATION_URL")

    compartment_ocid = os.getenv("COMPARTMENT_OCID")
    destination_url = os.getenv("DESTINATION_URL")

    input_location = oci.ai_document.models.ObjectLocation()
    input_location.namespace_name = os.getenv("NAMESPACE_NAME")
    input_location.bucket_name = os.getenv("BUCKET_NAME_IN")
    input_location.prefix = None
    input_location.object_name = None

    output_location = oci.ai_document.models.OutputLocation()
    output_location.namespace_name = os.getenv("NAMESPACE_NAME")
    output_location.bucket_name = os.getenv("BUCKET_NAME_OUT")
    output_location.prefix = None
    output_location.object_name = None


except Exception as e:
    logging.getLogger().error(e)
    raise


def format_document_json(json_input):
    for page in json_input["pages"]:
        page.pop("dimensions", None)
        page.pop("detectedDocumentTypes", None)
        page.pop("detectedLanguages", None)
        page.pop("words", None)
        page.pop("lines", None)
        page.pop("tables", None)

        if "documentFields" in page and page["documentFields"]:
            for i in range(len(page["documentFields"])):
                field = page["documentFields"][i]

                if field["fieldType"] == "KEY_VALUE":
                    field.pop("fieldName", None)
                    field["fieldValue"].pop("text", None)
                    field["fieldValue"].pop("confidence", None)
                    field["fieldValue"].pop("boundingPolygon", None)
                    field["fieldValue"].pop("wordIndexes", None)

                if field["fieldType"] == "LINE_ITEM_GROUP":
                    # page["documentFields"].pop(i)

                    for item in field["fieldValue"]["items"]:
                        item.pop("fieldLabel", None)
                        item.pop("fieldName", None)
                        item["fieldValue"].pop("text", None)
                        item["fieldValue"].pop("confidence", None)
                        item["fieldValue"].pop("boundingPolygon", None)
                        item["fieldValue"].pop("wordIndexes", None)

                        for item in item["fieldValue"]["items"]:
                            item.pop("fieldName", None)
                            item["fieldValue"].pop("text", None)
                            item["fieldValue"].pop("confidence", None)
                            item["fieldValue"].pop("boundingPolygon", None)
                            item["fieldValue"].pop("wordIndexes", None)

    json_input.pop("detectedDocumentTypes", None)
    json_input.pop("detectedLanguages", None)
    json_input.pop("documentClassificationModelVersion", None)
    json_input.pop("languageClassificationModelVersion", None)
    json_input.pop("textExtractionModelVersion", None)
    json_input.pop("keyValueExtractionModelVersion", None)
    json_input.pop("tableExtractionModelVersion", None)
    json_input.pop("errors", None)
    json_input.pop("searchablePdf", None)

    return json_input


def extract_key_value(body):
    document_id = None
    file_data = None
    file_type = None
    object_received = None
    document_json = None

    input_object_path = body["data"]["resourceName"]
    input_object_etag = body["data"]["additionalDetails"]["eTag"]
    input_object_name = input_object_path.split("/")[-1]

    if input_object_path.count("/") > 0:
        output_location.prefix = input_object_path.replace("/" + input_object_name, "")

    if input_object_path.count("/") > 1:
        document_id = input_object_path.split("/")[1]

    input_location.object_name = input_object_path

    key_value_detection_feature = (
        oci.ai_document.models.DocumentKeyValueExtractionFeature()
    )

    create_processor_job_details = oci.ai_document.models.CreateProcessorJobDetails(
        display_name=str(uuid.uuid4()),
        input_location=oci.ai_document.models.ObjectStorageLocations(
            object_locations=[input_location]
        ),
        output_location=output_location,
        compartment_id=compartment_ocid,
        processor_config=oci.ai_document.models.GeneralProcessorConfig(
            document_type=oci.ai_document.models.GeneralProcessorConfig.DOCUMENT_TYPE_INVOICE,
            features=[key_value_detection_feature],
            is_zip_output_enabled=False,
        ),
    )

    time_started = datetime.now().isoformat()

    create_processor_response = (
        ai_document_client.create_processor_job_and_wait_for_state(
            create_processor_job_details=create_processor_job_details,
            wait_for_states=[
                oci.ai_document.models.ProcessorJob.LIFECYCLE_STATE_SUCCEEDED
            ],
            operation_kwargs={"retry_strategy": oci.retry.NoneRetryStrategy()},
        )
    )

    time_finished = datetime.now().isoformat()

    # time_started = create_processor_response.data.time_started.isoformat()
    # time_finished = create_processor_response.data.time_finished.isoformat()

    if document_id is None:
        get_object = object_storage_client.get_object(
            input_location.namespace_name,
            input_location.bucket_name,
            input_location.object_name,
        )

        file_data = base64.b64encode(get_object.data.content).decode("utf-8")
        file_type = get_object.headers.get("content-type", None)
        object_received = get_object.headers.get("last-modified", None)

    output_location.object_name = "{}/{}/{}_{}/results/{}.json".format(
        output_location.prefix,
        create_processor_response.data.id,
        input_location.namespace_name,
        input_location.bucket_name,
        input_location.object_name,
    )

    get_object_response = object_storage_client.get_object(
        output_location.namespace_name,
        output_location.bucket_name,
        output_location.object_name,
    )

    document_json = json.loads(get_object_response.data.content)
    document_json = format_document_json(document_json)

    request_url = destination_url
    request_headers = {"content-type": "application/json"}
    request_json = {
        "id": document_id,
        "name": input_object_name,
        "data": file_data,
        "type": file_type,
        "etag": input_object_etag,
        "received": object_received,
        "job_id": create_processor_response.data.id,
        "start": time_started,
        "end": time_finished,
        "json": document_json,
    }

    response = requests.post(
        url=request_url,
        headers=request_headers,
        json=request_json,
    )


def handler(ctx, data: io.BytesIO = None):
    try:
        extract_key_value(json.loads(data.getvalue()))

    except Exception as handler_error:
        logging.getLogger().error(handler_error)
        return response.Response(
            ctx,
            status_code=500,
            response_data="Processing failed due to " + str(handler_error),
        )
    return response.Response(ctx, response_data="success")
