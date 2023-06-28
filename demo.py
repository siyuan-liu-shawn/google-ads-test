import hashlib

from google.ads.googleads.client import GoogleAdsClient

# init client
client = GoogleAdsClient.load_from_storage("./google-ads.yaml")
customer_id = '1493142199'


def main():
    user_list = create_customer_match_user_list(new=False)

    raw_records = get_records_from_bigquery()

    add_users_to_customer_match_user_list_by_job(user_list=user_list, raw_records=raw_records)


def create_customer_match_user_list(new):
    if not new:
        return 'customers/1493142199/userLists/8080319142'

    # Creates the UserListService client.
    user_list_service_client = client.get_service("UserListService")

    # Creates the user list operation.
    user_list_operation = client.get_type("UserListOperation")

    # Creates the new user list.
    user_list = user_list_operation.create
    user_list.name = "Customer Match list 1"
    user_list.description = (
        "A list of customers that originated from email and physical addresses"
    )

    ###################### dynamic setting???? #########################
    user_list.crm_based_user_list.upload_key_type = (
        client.enums.CustomerMatchUploadKeyTypeEnum.CONTACT_INFO
    )
    user_list.membership_life_span = 30

    response = user_list_service_client.mutate_user_lists(
        customer_id=customer_id, operations=[user_list_operation]
    )
    user_list_resource_name = response.results[0].resource_name
    print(
        f"User list with resource name '{user_list_resource_name}' was created."
    )

    return user_list_resource_name


def add_users_to_customer_match_user_list_by_job(user_list, raw_records):
    offline_user_data_job_service_client = client.get_service(
        "OfflineUserDataJobService"
    )

    # Create job
    offline_user_data_job = client.get_type("OfflineUserDataJob")
    offline_user_data_job.type_ = (
        client.enums.OfflineUserDataJobTypeEnum.CUSTOMER_MATCH_USER_LIST
    )
    offline_user_data_job.customer_match_user_list_metadata.user_list = (
        user_list
    )
    # Create an offline user data job.
    create_offline_user_data_job_response = offline_user_data_job_service_client.create_offline_user_data_job(
        customer_id=customer_id, job=offline_user_data_job
    )
    offline_user_data_job_resource_name = (
        create_offline_user_data_job_response.resource_name
    )
    print(
        "Created an offline user data job with resource name: "
        f"'{offline_user_data_job_resource_name}'."
    )

    # Add user to job
    request = client.get_type("AddOfflineUserDataJobOperationsRequest")
    request.resource_name = offline_user_data_job_resource_name
    for op in build_offline_user_data_job_operations(client, raw_records):
        request.operations.add().CopyFrom(op)
    request.enable_partial_failure = True
    response = offline_user_data_job_service_client.add_offline_user_data_job_operations(
        request=request
    )
    print_partial_failure(response)

    print("The operations are added to the offline user data job.")

    # Issues a request to run the offline user data job for executing all
    # added operations.
    offline_user_data_job_service_client.run_offline_user_data_job(
        resource_name=offline_user_data_job_resource_name
    )


def get_records_from_bigquery():
    # The first user data has an email address and a phone number.
    raw_record_1 = {
        "email": "dana@example.com",
        # Phone number to be converted to E.164 format, with a leading '+' as
        # required. This includes whitespace that will be removed later.
        "phone": "+1 800 5550101",
    }

    # The second user data has an email address, a mailing address, and a phone
    # number.
    raw_record_2 = {
        # Email address that includes a period (.) before the email domain.
        "email": "alex.2@example.com",
        # Address that includes all four required elements: first name, last
        # name, country code, and postal code.
        "first_name": "Alex",
        "last_mame": "Quinn",
        "country_code": "US",
        "postal_code": "94045",
        # Phone number to be converted to E.164 format, with a leading '+' as
        # required.
        "phone": "+1 800 5550102",
    }

    # The third user data only has an email address.
    raw_record_3 = {"email": "charlie@example.com"}

    # Adds the raw records to a raw input list.
    return [raw_record_1, raw_record_2, raw_record_3]


def build_offline_user_data_job_operations(client, raw_records):
    operations = []
    # Iterates over the raw input list and creates a UserData object for each
    # record.
    for record in raw_records:
        # Creates a UserData object that represents a member of the user list.
        user_data = client.get_type("UserData")

        if "email" in record:
            user_identifier = client.get_type("UserIdentifier")
            user_identifier.hashed_email = normalize_and_hash(
                record["email"], True
            )
            # Adds the hashed email identifier to the UserData object's list.
            user_data.user_identifiers.append(user_identifier)

        # Checks if the record has a phone number, and if so, adds a
        # UserIdentifier for it.
        if "phone" in record:
            user_identifier = client.get_type("UserIdentifier")
            user_identifier.hashed_phone_number = normalize_and_hash(
                record["phone"], True
            )
            # Adds the hashed phone number identifier to the UserData object's
            # list.
            user_data.user_identifiers.append(user_identifier)

        # Checks if the record has all the required mailing address elements,
        # and if so, adds a UserIdentifier for the mailing address.
        if "first_name" in record:
            required_keys = ("last_name", "country_code", "postal_code")
            # Checks if the record contains all the other required elements of
            # a mailing address.
            if not all(key in record for key in required_keys):
                # Determines which required elements are missing from the
                # record.
                missing_keys = record.keys() - required_keys
                print(
                    "Skipping addition of mailing address information "
                    "because the following required keys are missing: "
                    f"{missing_keys}"
                )
            else:
                user_identifier = client.get_type("UserIdentifier")
                address_info = user_identifier.address_info
                address_info.hashed_first_name = normalize_and_hash(
                    record["first_name"], False
                )
                address_info.hashed_last_name = normalize_and_hash(
                    record["first_last"], False
                )
                address_info.country_code = record["country_code"]
                address_info.postal_code = record["postal_code"]
                user_data.user_identifiers.append(user_identifier)

        # If the user_identifiers repeated field is not empty, create a new
        # OfflineUserDataJobOperation and add the UserData to it.
        if user_data.user_identifiers:
            operation = client.get_type("OfflineUserDataJobOperation")
            operation.create.CopyFrom(user_data)
            operations.append(operation)

    return operations


def normalize_and_hash(s, remove_all_whitespace):
    """Normalizes and hashes a string with SHA-256.

    Args:
        s: The string to perform this operation on.
        remove_all_whitespace: If true, removes leading, trailing, and
            intermediate spaces from the string before hashing. If false, only
            removes leading and trailing spaces from the string before hashing.

    Returns:
        A normalized (lowercase, remove whitespace) and SHA-256 hashed string.
    """
    # Normalizes by first converting all characters to lowercase, then trimming
    # spaces.
    if remove_all_whitespace:
        # Removes leading, trailing, and intermediate whitespace.
        s = "".join(s.split())
    else:
        # Removes only leading and trailing spaces.
        s = s.strip().lower()

    # Hashes the normalized string using the hashing algorithm.
    return hashlib.sha256(s.encode()).hexdigest()


def print_partial_failure(response):
    # Prints the status message if any partial failure error is returned.
    # Note: the details of each partial failure error are not printed here.
    # Refer to the error_handling/handle_partial_failure.py example to learn
    # more.
    # Extracts the partial failure from the response status.
    partial_failure = getattr(response, "partial_failure_error", None)
    if getattr(partial_failure, "code", None) != 0:
        error_details = getattr(partial_failure, "details", [])
        for error_detail in error_details:
            failure_message = client.get_type("GoogleAdsFailure")
            # Retrieve the class definition of the GoogleAdsFailure instance
            # in order to use the "deserialize" class method to parse the
            # error_detail string into a protobuf message object.
            failure_object = type(failure_message).deserialize(
                error_detail.value
            )

            for error in failure_object.errors:
                print(
                    "A partial failure at index "
                    f"{error.location.field_path_elements[0].index} occurred.\n"
                    f"Error message: {error.message}\n"
                    f"Error code: {error.error_code}"
                )


if __name__ == '__main__':
    main()
