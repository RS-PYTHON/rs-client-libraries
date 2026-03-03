# Copyright 2023-2026 Airbus, CS Group
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tables for the quota monitoring database"""

from sqlalchemy import TIMESTAMP, BigInteger, Column, Index, Integer, Text, text
from sqlalchemy.dialects.postgresql import INET
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):  # pylint: disable=too-few-public-methods
    """Base class for SQLAlchemy declarative models."""


class S3AccessLog(Base):  # pylint: disable=too-few-public-methods
    """
    ORM mapping for the s3_access_log table.
    Mirrors the PostgreSQL schema.
    """

    __tablename__ = "s3_access_log"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    bucket_owner = Column(Text)
    bucket = Column(Text, nullable=False)
    time = Column(TIMESTAMP, nullable=False)
    remote_ip = Column(INET)
    requester = Column(Text)
    request_id = Column(Text)
    operation = Column(Text)
    key = Column(Text)
    request_uri = Column(Text)
    http_status = Column(Integer)
    error_code = Column(Text)
    bytes_sent = Column(BigInteger)
    object_size = Column(BigInteger)
    total_time_ms = Column(Integer)
    turnaround_time_ms = Column(Integer)
    referer = Column(Text)
    user_agent = Column(Text)
    version_id = Column(Text)
    signature_version = Column(Text)
    authentication_type = Column(Text)
    host_header = Column(Text)

    __table_args__ = (
        # Composite index to speed up queries filtering on recent logs
        # and grouping by bucket (e.g., WHERE time >= NOW() - INTERVAL '30 days')
        Index("idx_s3log_time_bucket", "time", "bucket"),
        # Composite index to accelerate queries filtering by time and requester
        Index("idx_s3log_recent_requester", "time", "requester"),
        # Partial index for PUT operations (REST.PUT.PART)
        # Optimizes queries involving object_size for multipart uploads
        Index(
            "idx_s3log_put",
            "object_size",
            postgresql_where=(operation == "REST.PUT.PART"),
        ),
        # Partial index for GET operations (REST.GET.OBJECT)
        # Optimizes queries involving bytes_sent for object downloads
        Index(
            "idx_s3log_get",
            "bytes_sent",
            postgresql_where=(operation == "REST.GET.OBJECT"),
        ),
    )


class S3LogConsolidate(Base):  # pylint: disable=too-few-public-methods
    """
    ORM mapping for the s3_log_consolidate table.
    Mirrors the PostgreSQL schema.
    """

    __tablename__ = "s3_log_consolidate"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    bucket = Column(Text, nullable=False)
    time = Column(TIMESTAMP, nullable=False)
    requester = Column(Text)
    operation = Column(Text)

    bytes_sent = Column(BigInteger)
    object_size = Column(BigInteger)
    total_time_ms = Column(BigInteger)
    turnaround_time_ms = Column(BigInteger)

    created_at = Column(TIMESTAMP, nullable=False, server_default=text("now()"))

    __table_args__ = (
        # Index to accelerate recent-time queries by bucket
        Index("idx_s3log_consolidate_time_bucket", "time", "bucket"),
        # Composite index for analytics by requester and operation
        Index("idx_s3log_consolidate_time_requester_operation", "time", "requester", "operation"),
    )
