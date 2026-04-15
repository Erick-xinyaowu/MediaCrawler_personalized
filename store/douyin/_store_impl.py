# -*- coding: utf-8 -*-
# Copyright (c) 2025 relakkes@gmail.com
#
# This file is part of MediaCrawler project.
# Repository: https://github.com/NanmiCoder/MediaCrawler/blob/main/store/douyin/_store_impl.py
# GitHub: https://github.com/NanmiCoder
# Licensed under NON-COMMERCIAL LEARNING LICENSE 1.1
#

# 声明：本代码仅供学习和研究目的使用。使用者应遵守以下原则：
# 1. 不得用于任何商业用途。
# 2. 使用时应遵守目标平台的使用条款和robots.txt规则。
# 3. 不得进行大规模爬取或对平台造成运营干扰。
# 4. 应合理控制请求频率，避免给目标平台带来不必要的负担。
# 5. 不得用于任何非法或不当的用途。
#
# 详细许可条款请参阅项目根目录下的LICENSE文件。
# 使用本代码即表示您同意遵守上述原则和LICENSE中的所有条款。


# -*- coding: utf-8 -*-
# @Author  : persist1@126.com
# @Time    : 2025/9/5 19:34
# @Desc    : Douyin storage implementation class
import asyncio
import json
import os
import pathlib
from typing import Dict, Set

from sqlalchemy import select

import config
from base.base_crawler import AbstractStore
from database.db_session import get_session
from database.models import DouyinAweme, DouyinAwemeComment, DyCreator
from tools import utils, words
from tools.async_file_writer import AsyncFileWriter
from var import crawler_type_var
from database.mongodb_store_base import MongoDBStoreBase


class DouyinCsvStoreImplement(AbstractStore):
    def __init__(self):
        self.file_writer = AsyncFileWriter(
            crawler_type=crawler_type_var.get(),
            platform="douyin"
        )

    async def store_content(self, content_item: Dict):
        """
        Douyin content CSV storage implementation
        Args:
            content_item: note item dict

        Returns:

        """
        await self.file_writer.write_to_csv(
            item=content_item,
            item_type="contents"
        )

    async def store_comment(self, comment_item: Dict):
        """
        Douyin comment CSV storage implementation
        Args:
            comment_item: comment item dict

        Returns:

        """
        await self.file_writer.write_to_csv(
            item=comment_item,
            item_type="comments"
        )

    async def store_creator(self, creator: Dict):
        """
        Douyin creator CSV storage implementation
        Args:
            creator: creator item dict

        Returns:

        """
        await self.file_writer.write_to_csv(
            item=creator,
            item_type="creators"
        )


class DouyinDbStoreImplement(AbstractStore):
    async def store_content(self, content_item: Dict):
        """
        Douyin content DB storage implementation
        Args:
            content_item: content item dict
        """
        aweme_id = int(content_item.get("aweme_id"))
        async with get_session() as session:
            result = await session.execute(select(DouyinAweme).where(DouyinAweme.aweme_id == aweme_id))
            aweme_detail = result.scalar_one_or_none()

            if not aweme_detail:
                content_item["add_ts"] = utils.get_current_timestamp()
                if content_item.get("title"):
                    new_content = DouyinAweme(**content_item)
                    session.add(new_content)
            else:
                for key, value in content_item.items():
                    setattr(aweme_detail, key, value)
            await session.commit()

    async def store_comment(self, comment_item: Dict):
        """
        Douyin comment DB storage implementation
        Args:
            comment_item: comment item dict
        """
        comment_id = int(comment_item.get("comment_id"))
        async with get_session() as session:
            result = await session.execute(select(DouyinAwemeComment).where(DouyinAwemeComment.comment_id == comment_id))
            comment_detail = result.scalar_one_or_none()

            if not comment_detail:
                comment_item["add_ts"] = utils.get_current_timestamp()
                new_comment = DouyinAwemeComment(**comment_item)
                session.add(new_comment)
            else:
                for key, value in comment_item.items():
                    setattr(comment_detail, key, value)
            await session.commit()

    async def store_creator(self, creator: Dict):
        """
        Douyin creator DB storage implementation
        Args:
            creator: creator dict
        """
        user_id = creator.get("user_id")
        async with get_session() as session:
            result = await session.execute(select(DyCreator).where(DyCreator.user_id == user_id))
            user_detail = result.scalar_one_or_none()

            if not user_detail:
                creator["add_ts"] = utils.get_current_timestamp()
                new_creator = DyCreator(**creator)
                session.add(new_creator)
            else:
                for key, value in creator.items():
                    setattr(user_detail, key, value)
            await session.commit()


class DouyinJsonStoreImplement(AbstractStore):
    def __init__(self):
        self.file_writer = AsyncFileWriter(
            crawler_type=crawler_type_var.get(),
            platform="douyin"
        )

    async def store_content(self, content_item: Dict):
        """
        content JSON storage implementation
        Args:
            content_item:

        Returns:

        """
        await self.file_writer.write_single_item_to_json(
            item=content_item,
            item_type="contents"
        )

    async def store_comment(self, comment_item: Dict):
        """
        comment JSON storage implementation
        Args:
            comment_item:

        Returns:

        """
        await self.file_writer.write_single_item_to_json(
            item=comment_item,
            item_type="comments"
        )

    async def store_creator(self, creator: Dict):
        """
        creator JSON storage implementation
        Args:
            creator:

        Returns:

        """
        await self.file_writer.write_single_item_to_json(
            item=creator,
            item_type="creators"
        )



class DouyinJsonlStoreImplement(AbstractStore):
    def __init__(self):
        self.file_writer = AsyncFileWriter(
            crawler_type=crawler_type_var.get(),
            platform="douyin"
        )
        self._loaded_existing_ids = False
        self._load_lock = asyncio.Lock()
        self._existing_aweme_ids: Set[str] = set()
        self._existing_comment_ids: Set[str] = set()

    async def _load_existing_ids_once(self):
        """Load IDs from existing JSONL files once to support incremental crawling."""
        if self._loaded_existing_ids:
            return

        async with self._load_lock:
            if self._loaded_existing_ids:
                return

            contents_path = self.file_writer._get_file_path("jsonl", "contents")
            comments_path = self.file_writer._get_file_path("jsonl", "comments")

            def _load_ids(file_path: str, field_name: str, target_set: Set[str]):
                if not os.path.exists(file_path):
                    return
                with open(file_path, "r", encoding="utf-8") as f:
                    for line in f:
                        raw = line.strip()
                        if not raw:
                            continue
                        try:
                            item = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        value = item.get(field_name)
                        if value is not None:
                            target_set.add(str(value))

            _load_ids(contents_path, "aweme_id", self._existing_aweme_ids)
            _load_ids(comments_path, "comment_id", self._existing_comment_ids)

            self._loaded_existing_ids = True
            utils.logger.info(
                f"[DouyinJsonlStoreImplement] Incremental mode loaded: "
                f"{len(self._existing_aweme_ids)} aweme_ids, {len(self._existing_comment_ids)} comment_ids"
            )

    async def store_content(self, content_item: Dict):
        await self._load_existing_ids_once()

        aweme_id = content_item.get("aweme_id")
        if aweme_id is None:
            return
        aweme_id_str = str(aweme_id)
        if aweme_id_str in self._existing_aweme_ids:
            return

        await self.file_writer.write_to_jsonl(
            item=content_item,
            item_type="contents"
        )
        self._existing_aweme_ids.add(aweme_id_str)

    async def store_comment(self, comment_item: Dict):
        await self._load_existing_ids_once()

        comment_id = comment_item.get("comment_id")
        if comment_id is None:
            return
        comment_id_str = str(comment_id)
        if comment_id_str in self._existing_comment_ids:
            return

        await self.file_writer.write_to_jsonl(
            item=comment_item,
            item_type="comments"
        )
        self._existing_comment_ids.add(comment_id_str)

    async def store_creator(self, creator: Dict):
        await self.file_writer.write_to_jsonl(
            item=creator,
            item_type="creators"
        )


class DouyinSqliteStoreImplement(DouyinDbStoreImplement):
    pass


class DouyinMongoStoreImplement(AbstractStore):
    """Douyin MongoDB storage implementation"""

    def __init__(self):
        self.mongo_store = MongoDBStoreBase(collection_prefix="douyin")

    async def store_content(self, content_item: Dict):
        """
        Store video content to MongoDB
        Args:
            content_item: Video content data
        """
        aweme_id = content_item.get("aweme_id")
        if not aweme_id:
            return

        await self.mongo_store.save_or_update(
            collection_suffix="contents",
            query={"aweme_id": aweme_id},
            data=content_item
        )
        utils.logger.info(f"[DouyinMongoStoreImplement.store_content] Saved aweme {aweme_id} to MongoDB")

    async def store_comment(self, comment_item: Dict):
        """
        Store comment to MongoDB
        Args:
            comment_item: Comment data
        """
        comment_id = comment_item.get("comment_id")
        if not comment_id:
            return

        await self.mongo_store.save_or_update(
            collection_suffix="comments",
            query={"comment_id": comment_id},
            data=comment_item
        )
        utils.logger.info(f"[DouyinMongoStoreImplement.store_comment] Saved comment {comment_id} to MongoDB")

    async def store_creator(self, creator_item: Dict):
        """
        Store creator information to MongoDB
        Args:
            creator_item: Creator data
        """
        user_id = creator_item.get("user_id")
        if not user_id:
            return

        await self.mongo_store.save_or_update(
            collection_suffix="creators",
            query={"user_id": user_id},
            data=creator_item
        )
        utils.logger.info(f"[DouyinMongoStoreImplement.store_creator] Saved creator {user_id} to MongoDB")


class DouyinExcelStoreImplement:
    """Douyin Excel storage implementation - Global singleton"""

    def __new__(cls, *args, **kwargs):
        from store.excel_store_base import ExcelStoreBase
        return ExcelStoreBase.get_instance(
            platform="douyin",
            crawler_type=crawler_type_var.get()
        )
