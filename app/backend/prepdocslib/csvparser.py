import csv
import logging
from collections.abc import AsyncGenerator
from typing import IO

from .page import Page
from .parser import Parser

logger = logging.getLogger("scripts")


class CsvParser(Parser):
    """
    Concrete parser that can parse CSV into Page objects. Each row becomes a Page object.
    Uses only the 'Content' column for embedding if available, otherwise falls back to all columns.
    """

    async def parse(self, content: IO) -> AsyncGenerator[Page, None]:
        # Check if content is in bytes (binary file) and decode to string
        content_str: str
        if isinstance(content, (bytes, bytearray)):
            content_str = content.decode("utf-8")
        elif hasattr(content, "read"):  # Handle BufferedReader
            content_str = content.read().decode("utf-8")

        # Create a CSV reader from the text content
        reader = csv.reader(content_str.splitlines())
        offset = 0

        # Read the header row to find Content column index
        headers = next(reader, [])
        content_column_index = None
        
        # Find the Content column (case-insensitive)
        for i, header in enumerate(headers):
            if header.strip().lower() == 'content':
                content_column_index = i
                logger.info("Found 'Content' column at index %d, using only this column for embeddings", i)
                break
        
        # If no Content column found, fall back to all columns
        if content_column_index is None:
            logger.warning("No 'Content' column found in CSV, using all columns for embeddings")

        for i, row in enumerate(reader):
            if content_column_index is not None and len(row) > content_column_index:
                # Use only the Content column
                page_text = row[content_column_index]
            else:
                # Fallback to all columns if Content column not available
                page_text = ",".join(row)
            
            yield Page(i, offset, page_text)
            offset += len(page_text) + 1  # Account for newline character
