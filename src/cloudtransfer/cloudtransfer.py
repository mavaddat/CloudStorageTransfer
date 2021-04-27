from _typeshed import ReadableBuffer
import logging
import boto3
import json
import dropbox
import sys
from botocore.exceptions import ClientError
from dropbox import files
from dropbox.files import WriteMode
from dropbox.exceptions import ApiError, AuthError

LOGGING_FORMAT = '%(levelname)s: %(asctime)s: %(message)s'

# Add OAuth2 access token here.
TOKEN = ''

# You can generate one for yourself in the App Console.
# See https://dropbox.tech/developers/generate-an-access-token-for-your-own-account

LOCALFILE = 'my-file.txt' # The file to backup
BACKUPPATH = '/my-file-backup.txt' # The cloud destination


# Uploads contents of LOCALFILE to Dropbox
def backup():
    with open(LOCALFILE, 'rb') as f, dropbox.Dropbox(TOKEN) as dbx:
        # We use WriteMode=overwrite to make sure that the settings in the file
        # are changed on upload
        print("Uploading " + LOCALFILE + " to Dropbox as " + BACKUPPATH + "…")
        try:
            dbx.files_upload(
                f.read(), BACKUPPATH, mode=WriteMode('overwrite')
            )
        except ApiError as err:
            # This checks for the specific error where a user doesn't have
            # enough Dropbox space quota to upload this file
            if (
                err.error.is_path()
                and err.error.get_path().reason.is_insufficient_space()
            ):
                sys.exit("ERROR: Cannot back up; insufficient space.")
            elif err.user_message_text:
                print(err.user_message_text)
                sys.exit()
            else:
                print(err)
                sys.exit()


# Change the text string in LOCALFILE to be new_content
# @param new_content is a string
def change_local_file(new_content:ReadableBuffer):
    print("Changing contents of " + LOCALFILE + " on local machine…")
    with open(LOCALFILE, 'wb') as f:
        f.write(new_content)


# Restore the local and Dropbox files to a certain revision
def restore(rev=None):
    with dropbox.Dropbox(TOKEN) as dbx:
        # Restore the file on Dropbox to a certain revision
        print(
            "Restoring " + BACKUPPATH + " to revision " + rev + " on Dropbox…"
        )
        dbx.files_restore(BACKUPPATH, rev)

        # Download the specific revision of the file at BACKUPPATH to LOCALFILE
        print(
            "Downloading current " + BACKUPPATH
            + " from Dropbox, overwriting " + LOCALFILE + "…"
        )
        dbx.files_download_to_file(LOCALFILE, BACKUPPATH, rev)


# Look at all of the available revisions on Dropbox, and return the oldest one
def select_revision():
    """Look at all of the available revisions on Dropbox, and return the oldest one

    Returns:
        [type]: [description]
    """    
    with dropbox.Dropbox(TOKEN) as dbx:
        # Get the revisions for file
        #   and sort by datetime object, "server_modified"
        print("Finding available revisions on Dropbox…")
        revision_result = dbx.files_list_revisions(BACKUPPATH, limit=30)
        if isinstance(revision_result,files.ListRevisionsResult):
            revision_result = revision_result.entries
            revisions = sorted(revision_result, key=lambda entry: entry.server_modified)

            for revision in revisions:
                print(revision.rev, revision.server_modified)

            # Return oldest revision
            #   first entry, ∵ revisions sorted oldest:newest
            return revisions[0].rev
        else:
            return None

def test_backup_and_restore():
    # Check for an access token
    if (len(TOKEN) == 0):
        sys.exit(
            "ERROR: Looks like you didn't add your access token. "
            "Open up backup-and-restore-example.py in a text editor and "
            "paste in your token in line 14.")

    # Create an instance of a Dropbox class, which can make requests to API.
    print("Creating a Dropbox object…")
    with dropbox.Dropbox(TOKEN) as dbx:

        # Check that the access token is valid
        try:
            dbx.users_get_current_account()
        except AuthError:
            sys.exit(
                "ERROR: Invalid access token; try re-generating an "
                "access token from the app console on the web.")

        # Create a backup of the current settings file
        backup()

        # Change the user's file, create another backup
        change_local_file(b"updated")
        backup()

        # Restore the local and Dropbox files to a certain revision
        to_rev = select_revision()
        restore(to_rev)

        print("Done!")


def create_vault(vault_name):
    """Create an Amazon Glacier vault.

    :param vault_name: string
    :return: glacier.Vault object if vault was created, otherwise None
    """

    glacier = boto3.resource('glacier')
    try:
        vault = glacier.create_vault(vaultName=vault_name)
    except ClientError as e:
        logging.error(e)
        return None
    return vault


def test_create_vault():
    """ Exercise create_vault()"""

    # Assign this value before running the program
    test_vault_name = 'VAULT_NAME'

    # Set up logging
    logging.basicConfig(level=logging.DEBUG,
                        format=LOGGING_FORMAT)

    # Create the Glacier vault
    vault = create_vault(test_vault_name)
    if vault is not None:
        logging.info(f'Created vault {vault.name}')


def delete_vault(vault_name):
    """Delete an Amazon S3Glacier vault
    :param vault_name: string
    :return: True if vault was deleted, otherwise False
    """

    # Delete the vault
    glacier = boto3.client('glacier')
    try:
        response = glacier.delete_vault(vaultName=vault_name)
        logging.info(f'Received response {response} from {vault_name}')
    except ClientError as e:
        logging.error(e)
        return False
    return True


def test_delete_vault():
    """Exercise delete_vault()"""

    # Assign this value before running the program
    test_vault_name = 'VAULT_NAME'

    # Set up logging
    logging.basicConfig(level=logging.DEBUG,
                        format=LOGGING_FORMAT)

    # Delete the vault
    success = delete_vault(test_vault_name)
    if success:
        logging.info(f'Deleted vault {test_vault_name}')


def delete_archive(vault_name, archive_id):
    """Delete an archive from an Amazon S3 Glacier vault

    :param vault_name: string
    :param archive_id: string
    :return: True if archive was deleted, otherwise False
    """

    # Delete the archive
    glacier = boto3.client('glacier')
    try:
        response = glacier.delete_archive(vaultName=vault_name,
                                          archiveId=archive_id)
        logging.info(
            f'Received response {response} from '
            + '{vault_name} on archive id {archive_id}'
        )
    except ClientError as e:
        logging.error(e)
        return False
    return True


def test_delete_archive():
    """Exercise delete_vault()"""

    # Assign these values before running the program
    test_vault_name = 'VAULT_NAME'
    test_archive_id = 'ARCHIVE_ID'

    # Set up logging
    logging.basicConfig(level=logging.DEBUG,
                        format=LOGGING_FORMAT)

    # Delete the archive
    success = delete_archive(test_vault_name, test_archive_id)
    if success:
        logging.info(
            f'Deleted archive {test_archive_id} from {test_vault_name}'
        )


def describe_job(vault_name, job_id):
    """Retrieve the status of an Amazon S3 Glacier job, such as an
    inventory-retrieval job
    To retrieve output of finished job, call Glacier.Client.get_job_output()
    :param vault_name: string
    :param job_id: string. Job ID returned by Glacier.Client.initiate_job().
    :return: Dictionary of information related to job. If error, return None.
    """

    # Retrieve the status of the job
    glacier = boto3.client('glacier')
    try:
        response = glacier.describe_job(vaultName=vault_name, jobId=job_id)
    except ClientError as e:
        logging.error(e)
        return None
    return response


def test_describe_job():
    """Exercise describe_job()"""

    # Assign the following values before running the program
    test_vault_name = 'VAULT_NAME'
    test_job_id = 'JOB_ID'

    # Set up logging
    logging.basicConfig(level=logging.DEBUG,
                        format=LOGGING_FORMAT)

    # Retrieve the job's status
    response = describe_job(test_vault_name, test_job_id)
    if response is not None:
        logging.info(f'Job Type: {response["Action"]}, '
                     f'Status: {response["StatusCode"]}')


def list_vaults(max_vaults=10, iter_marker=None):
    """List Amazon S3 Glacier vaults owned by the AWS account
    :param max_vaults: Maximum number of vaults to retrieve
    :param iter_marker: Marker used to identify start of next batch of vaults
    to retrieve
    :return: List of dictionaries containing vault information
    :return: String marking the start of next batch of vaults to retrieve.
    Pass this string as the iter_marker argument in the next invocation of
    list_vaults().
    """

    # Retrieve vaults
    glacier = boto3.client('glacier')
    if iter_marker is None:
        vaults = glacier.list_vaults(limit=str(max_vaults))
    else:
        vaults = glacier.list_vaults(limit=str(max_vaults), marker=iter_marker)
    marker = vaults.get('Marker')       # None if no more vaults to retrieve
    return vaults['VaultList'], marker


def test_list_vaults():
    """Exercise list_vaults()"""

    # Set up logging
    logging.basicConfig(level=logging.DEBUG,
                        format=LOGGING_FORMAT)

    # List the vaults
    vaults, marker = list_vaults()
    while True:
        # Print info about retrieved vaults
        for vault in vaults:
            logging.info(f'{vault["NumberOfArchives"]:3d}  '
                         f'{vault["SizeInBytes"]:12d}  {vault["VaultName"]}')

        # If no more vaults exist, exit loop, otherwise retrieve the next batch
        if marker is None:
            break
        vaults, marker = list_vaults(iter_marker=marker)


def retrieve_inventory(vault_name):
    """Initiate an Amazon Glacier inventory-retrieval job
    To check the status of the job, call Glacier.Client.describe_job()
    To retrieve the output of the job, call Glacier.Client.get_job_output()
    :param vault_name: string
    :return: Dictionary of information related to the initiated job. If error,
    returns None.
    """

    # Construct job parameters
    job_parms = {'Type': 'inventory-retrieval'}

    # Initiate the job
    glacier = boto3.client('glacier')
    try:
        response = glacier.initiate_job(vaultName=vault_name,
                                        jobParameters=job_parms)
    except ClientError as e:
        logging.error(e)
        return None
    return response


def test_retrieve_inventory():
    """Exercise retrieve_inventory()"""

    # Assign this value before running the program
    test_vault_name = 'VAULT_NAME'

    # Set up logging
    logging.basicConfig(level=logging.DEBUG,
                        format=LOGGING_FORMAT)

    # Initiate an inventory retrieval job
    response = retrieve_inventory(test_vault_name)
    if response is not None:
        logging.info(
            f'Initiated inventory-retrieval job for {test_vault_name}'
        )
        logging.info(f'Retrieval Job ID: {response["jobId"]}')


def retrieve_inventory_results(vault_name, job_id):
    """Retrieve the results of an Amazon Glacier inventory-retrieval job
    :param vault_name: string
    :param job_id: string. Job ID was returned by Glacier.Client.initiate_job()
    :return: Dictionary containing the results of the inventory-retrieval job.
    If error, return None.
    """

    # Retrieve the job results
    glacier = boto3.client('glacier')
    try:
        response = glacier.get_job_output(vaultName=vault_name, jobId=job_id)
    except ClientError as e:
        logging.error(e)
        return None

    # Read the streaming results into a dictionary
    return json.loads(response['body'].read())


def test_retrieve_inventory_results():
    """Exercise retrieve_inventory_result()"""

    # Assign these values before running the program
    test_vault_name = 'VAULT_NAME'
    test_job_id = 'JOB_ID'

    # Set up logging
    logging.basicConfig(level=logging.DEBUG,
                        format=LOGGING_FORMAT)

    # Retrieve the job results
    inventory = retrieve_inventory_results(test_vault_name, test_job_id)
    if inventory is not None:
        # Output some of the inventory information
        logging.info(f'Vault ARN: {inventory["VaultARN"]}')
        for archive in inventory['ArchiveList']:
            logging.info(f'  Size: {archive["Size"]:6d}  '
                         f'Archive ID: {archive["ArchiveId"]}')


def upload_archive(vault_name, src_data):
    """Add an archive to an Amazon S3 Glacier vault.
    The upload occurs synchronously.
    :param vault_name: string
    :param src_data: bytes of data or string reference to file spec
    :return: If src_data was added to vault, return dict of archive
    information, otherwise None
    """

    # The src_data argument must be of type bytes or string
    # Construct body= parameter
    if isinstance(src_data, bytes):
        object_data = src_data
    elif isinstance(src_data, str):
        try:
            object_data = open(src_data, 'rb')
            # possible FileNotFoundError/IOError exception
        except Exception as e:
            logging.error(e)
            return None
    else:
        logging.error(
            'Type of ' + str(type(src_data))
            + ' for the argument \'src_data\' is not supported.'
        )
        return None

    glacier = boto3.client('glacier')
    try:
        archive = glacier.upload_archive(vaultName=vault_name,
                                         body=object_data)
    except ClientError as e:
        logging.error(e)
        return None
    finally:
        if isinstance(src_data, str):
            object_data.close()

    # Return dictionary of archive information
    return archive


def test_upload_archive():
    """Exercise upload_archive()"""

    # Assign these values before running the program
    test_vault_name = 'VAULT_NAME'
    filename = 'C:\\path\\to\\filename.ext'
    # Alternatively, specify object contents using bytes.
    # filename = b'This is the data to store in the Glacier archive.'

    # Set up logging
    logging.basicConfig(level=logging.DEBUG,
                        format=LOGGING_FORMAT)

    # Upload the archive
    archive = upload_archive(test_vault_name, filename)
    if archive is not None:
        logging.info(
            f'Archive {archive["archiveId"]} added to {test_vault_name}'
        )


def main():
    test_create_vault()
    test_delete_archive()
    test_delete_vault()
    test_describe_job()
    test_list_vaults()
    test_retrieve_inventory()
    test_retrieve_inventory_results()
    test_upload_archive()
    test_backup_and_restore()


if __name__ == '__main__':
    main()
