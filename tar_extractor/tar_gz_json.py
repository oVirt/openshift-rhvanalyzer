import tarfile
from io import BytesIO

import logging

logger = logging.getLogger()


def _only_json_file(member: tarfile.TarInfo) -> bool:
    """Selector for JSON files only in a TAR file."""
    return member.name.endswith('.json')


async def json_extractor(file_obj: BytesIO) -> bytes:
    """Extract JSON data from TAR.GZ file.
    Extracts all JSON files from a TAR.GZ archive and combines them into a list
    of dictionary objects.
    :param filename: Name of the filename to parse
    """
    with tarfile.open(fileobj=file_obj) as tar:
        # Read just the first JSON available
        member = next(filter(_only_json_file, tar.members), None)
        # Return if no JSON file was found
        if not member:
            return

        # Extract JSON file object
        json_data = tar.extractfile(member)
        return json_data.read()
