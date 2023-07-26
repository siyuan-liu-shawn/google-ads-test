import hashlib
from typing import Dict

from google.ads.googleads.client import GoogleAdsClient


class BigQueryToGoogleAdsCustomerMatchTask:
    def __init__(
            self,
            project: str,
            customer_id: str,
            user_list_id: str,
            query: str,
            **kwargs,
    ):
        self.customer_id = customer_id
        self.user_list_id = user_list_id
        self.query = query

        super(BigQueryToGoogleAdsCustomerMatchTask, self).__init__(
            project=project,
            query=self.query,
        )

        serialized = self.get_credential()
        self.client = GoogleAdsApiClient.load_from_storage("./google-ads.yaml")


    @staticmethod
    def get_credential() -> Dict[str, str]:
        client_id = "111282808196-9jjt01ciee8apckln54vvbq7ljoecas6.apps.googleusercontent.com"
        login_customer_id = "3413861815"
        client_secret_id = (
            "projects/518261476924/secrets/"
            "adhoc-to-ads-customer-match-client-secret/versions/latest"
        )
        refresh_token_id = (
            "projects/518261476924/secrets/"
            "adhoc-to-ads-customer-match-refresh-token/versions/latest"
        )
        developer_token_id = (
            "projects/518261476924/secrets/"
            "adhoc-to-ads-customer-match-developer-token/versions/latest"
        )

        return {
            "client_id": client_id,
            "use_proto_plus": "False",
            "client_secret": get_gcp_secret(client_secret_id),
            "refresh_token": get_gcp_secret(refresh_token_id),
            "developer_token": get_gcp_secret(developer_token_id),
            "login_customer_id": login_customer_id,
        }

    def request_from_data(self, df: DataFrame) -> List[Any]:
        # Get data
        records_from_bq = cast(List[Dict[str, str]], df.to_dict(orient="records"))

        # Transform data
        upload_key_type = self.client.get_user_list_upload_key_type(
            self.customer_id, self.user_list_id
        )
        if upload_key_type == CustomerMatchUploadKeyType.CONTACT_INFO:
            offline_job_operations = self.transform_to_contact_info_job_operation(records_from_bq)

        elif upload_key_type == CustomerMatchUploadKeyType.CRM_ID:
            offline_job_operations = self.transform_to_crm_id_job_operation(records_from_bq)

        elif upload_key_type == CustomerMatchUploadKeyType.MOBILE_ADVERTISING_ID:
            offline_job_operations = self.transform_to_mobile_id_job_operation(records_from_bq)

        else:
            self.logger.info(
                msg=f"The upload_key_type {upload_key_type} is unknown."
                    f"Therefore, skip the operation of customer_id: {self.customer_id} and"
                    f"user_list_id: {self.user_list_id}"
            )
            return []

        # Save data to google
        user_list_resource_name = self.client.get_user_list_resource_name(
            customer_id=self.customer_id,
            user_list_id=self.user_list_id,
        )
        return self.client.add_users_to_customer_match_user_list(
            self.customer_id, user_list_resource_name, offline_job_operations
        )

    def transform_to_contact_info_job_operation(self, records_from_bq) -> List[Any]:
        """
        Transform raw records to google-ads api operations.
        The record is contact info type including "email", "phone",
        "first_name", "last_name", "country_code", and "postal_code".
        """
        self.logger.info("Transform records to contact info operations.")
        operations = []
        # Iterates over the raw input list and creates a UserData object for each
        # record.
        for record in records_from_bq:
            # Creates a UserData object that represents a member of the user list.
            user_data = self.client.get_type("UserData")

            if "email" in record:
                user_identifier = self.client.get_type("UserIdentifier")
                user_identifier.hashed_email = record["email"]
                # Adds the hashed email identifier to the UserData object's list.
                user_data.user_identifiers.append(user_identifier)

            # Checks if the record has a phone number, and if so, adds a
            # UserIdentifier for it.
            if "phone" in record:
                user_identifier = self.client.get_type("UserIdentifier")
                user_identifier.hashed_phone_number = record["phone"]
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
                    self.logger.info(
                        "Skipping addition of mailing address information "
                        "because the following required keys are missing: "
                        f"{missing_keys}"
                    )
                else:
                    user_identifier = self.client.get_type("UserIdentifier")
                    address_info = user_identifier.address_info
                    address_info.hashed_last_name = record["last_name"]
                    address_info.country_code = record["country_code"]
                    address_info.postal_code = record["postal_code"]
                    user_data.user_identifiers.append(user_identifier)

            # If the user_identifiers repeated field is not empty, create a new
            # OfflineUserDataJobOperation and add the UserData to it.
            if user_data.user_identifiers:
                operation = self.client.get_type("OfflineUserDataJobOperation")
                operation.create.CopyFrom(user_data)
                operations.append(operation)

        return operations

    def transform_to_crm_id_job_operation(self, records_from_bq):
        # TODO(siyuan): Implement in other PR
        return records_from_bq

    def transform_to_mobile_id_job_operation(self, records_from_bq):
        # TODO(siyuan): Implement in other PR
        return records_from_bq


def main():
    print(123)


if __name__ == '__main__':
    main()
