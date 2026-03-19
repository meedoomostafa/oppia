# coding: utf-8
#
# Copyright 2026 The Oppia Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS-IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Jobs that audit and clean up invalid UnsentFeedbackEmailModel references.

This module provides Beam jobs to handle orphaned references in
UnsentFeedbackEmailModel instances. These references can become orphaned when
feedback threads or messages are deleted but the corresponding email
notification references are not cleaned up.

See GitHub issue #14966 for context.
"""

from __future__ import annotations

from core.jobs import base_jobs
from core.jobs.io import ndb_io
from core.jobs.types import job_run_result
from core.platform import models

import apache_beam as beam

from typing import List, Set, Tuple

MYPY = False
if MYPY:  # pragma: no cover
    from mypy_imports import feedback_models

(feedback_models,) = models.Registry.import_models([models.Names.FEEDBACK])


class CleanupInvalidUnsentFeedbackReferencesJob(base_jobs.JobBase):
    """Cleans up invalid references in UnsentFeedbackEmailModel instances.

    This job scans all UnsentFeedbackEmailModel instances and removes
    references to threads and messages that no longer exist. If an
    UnsentFeedbackEmailModel has no valid references remaining, it is deleted.

    When DATASTORE_UPDATES_ALLOWED is False, this job behaves as an audit job
    and only reports invalid models without mutating the datastore.
    """

    DATASTORE_UPDATES_ALLOWED = True

    def run(self) -> beam.PCollection[job_run_result.JobRunResult]:
        """Runs the job.

        Returns:
            PCollection[JobRunResult]. Results from audit or cleanup.
        """
        # Get all existing thread IDs.
        thread_ids = (
            self.pipeline
            | 'Get GeneralFeedbackThreadModels'
            >> ndb_io.GetModels(
                feedback_models.GeneralFeedbackThreadModel.get_all(
                    include_deleted=False
                )
            )
            | 'Extract thread ids' >> beam.Map(lambda model: model.id)
        )

        # Get all existing message IDs as (thread_id, message_id) tuples.
        message_ids = (
            self.pipeline
            | 'Get GeneralFeedbackMessageModels'
            >> ndb_io.GetModels(
                feedback_models.GeneralFeedbackMessageModel.get_all(
                    include_deleted=False
                )
            )
            | 'Extract message ids'
            >> beam.Map(lambda model: (model.thread_id, model.message_id))
        )

        # Get all UnsentFeedbackEmailModels.
        unsent_email_models = (
            self.pipeline
            | 'Get UnsentFeedbackEmailModels'
            >> ndb_io.GetModels(
                feedback_models.UnsentFeedbackEmailModel.get_all(
                    include_deleted=False
                )
            )
        )

        # Find models with invalid references.
        models_with_invalid_refs = (
            unsent_email_models
            | 'Find models with invalid references'
            >> beam.Filter(
                self._has_invalid_references,
                beam.pvalue.AsList(thread_ids),
                beam.pvalue.AsList(message_ids),
            )
        )

        # Count invalid references for reporting.
        invalid_ref_details = (
            models_with_invalid_refs
            | 'Extract invalid reference details'
            >> beam.FlatMap(
                self._get_invalid_reference_details,
                beam.pvalue.AsList(thread_ids),
                beam.pvalue.AsList(message_ids),
            )
        )

        invalid_ref_logs = (
            invalid_ref_details
            | 'Log invalid references'
            >> beam.Map(
                lambda detail: job_run_result.JobRunResult.as_stdout(
                    (
                        'Invalid reference in UnsentFeedbackEmailModel: '
                        f'user_id={detail[0]}, '
                        f'thread_id={detail[1]}, '
                        f'message_id={detail[2]}, '
                        f'reason={detail[3]}'
                    )
                )
            )
        )

        invalid_ref_count = (
            invalid_ref_details
            | 'Count invalid references'
            >> beam.combiners.Count.Globally().with_defaults(0)
            | 'Report invalid reference count'
            >> beam.Map(
                lambda count: job_run_result.JobRunResult.as_stdout(
                    f'invalid_references_count: {count}'
                )
            )
        )

        affected_model_count = (
            models_with_invalid_refs
            | 'Count affected models'
            >> beam.combiners.Count.Globally().with_defaults(0)
            | 'Report affected model count'
            >> beam.Map(
                lambda count: job_run_result.JobRunResult.as_stdout(
                    f'affected_unsent_email_models_count: {count}'
                )
            )
        )

        outputs: List[beam.PCollection[job_run_result.JobRunResult]] = []

        if self.DATASTORE_UPDATES_ALLOWED:
            # Separate models to delete vs update.
            models_to_delete = (
                models_with_invalid_refs
                | 'Find models to delete'
                >> beam.Filter(
                    self._should_delete_model,
                    beam.pvalue.AsList(thread_ids),
                    beam.pvalue.AsList(message_ids),
                )
            )

            models_to_update = (
                models_with_invalid_refs
                | 'Find models to update'
                >> beam.Filter(
                    lambda model, valid_threads, valid_messages: (
                        not self._should_delete_model(
                            model, valid_threads, valid_messages
                        )
                    ),
                    beam.pvalue.AsList(thread_ids),
                    beam.pvalue.AsList(message_ids),
                )
            )

            # Delete models with no valid references.
            deleted_model_logs = (
                models_to_delete
                | 'Log deleted models'
                >> beam.Map(
                    lambda model: job_run_result.JobRunResult.as_stdout(
                        (
                            'Deleted UnsentFeedbackEmailModel (no valid refs): '
                            f'user_id={model.id}'
                        )
                    )
                )
            )

            deleted_model_count = (
                models_to_delete
                | 'Count deleted models'
                >> beam.combiners.Count.Globally().with_defaults(0)
                | 'Report deleted model count'
                >> beam.Map(
                    lambda count: job_run_result.JobRunResult.as_stdout(
                        f'deleted_unsent_email_models_count: {count}'
                    )
                )
            )

            unused_delete_results = (
                models_to_delete
                | 'Extract model keys for deletion'
                >> beam.Map(lambda model: model.key)
                | 'Delete models' >> ndb_io.DeleteModels()
            )

            # Update models with some valid references.
            updated_model_logs = models_to_update | 'Log updated models' >> beam.Map(
                lambda model: job_run_result.JobRunResult.as_stdout(
                    (
                        'Updated UnsentFeedbackEmailModel (removed invalid refs): '
                        f'user_id={model.id}'
                    )
                )
            )

            updated_model_count = (
                models_to_update
                | 'Count updated models'
                >> beam.combiners.Count.Globally().with_defaults(0)
                | 'Report updated model count'
                >> beam.Map(
                    lambda count: job_run_result.JobRunResult.as_stdout(
                        f'updated_unsent_email_models_count: {count}'
                    )
                )
            )

            unused_update_results = (
                models_to_update
                | 'Clean invalid references'
                >> beam.Map(
                    self._clean_invalid_references,
                    beam.pvalue.AsList(thread_ids),
                    beam.pvalue.AsList(message_ids),
                )
                | 'Put updated models' >> ndb_io.PutModels()
            )

            outputs.extend(
                [
                    deleted_model_logs,
                    deleted_model_count,
                    updated_model_logs,
                    updated_model_count,
                ]
            )
        else:
            outputs.extend(
                [
                    invalid_ref_logs,
                    invalid_ref_count,
                    affected_model_count,
                ]
            )

        return outputs | 'Flatten outputs' >> beam.Flatten()

    @staticmethod
    def _has_invalid_references(
        model: feedback_models.UnsentFeedbackEmailModel,
        valid_thread_ids: List[str],
        valid_message_ids: List[Tuple[str, int]],
    ) -> bool:
        """Checks if the model has any invalid references.

        Args:
            model: UnsentFeedbackEmailModel. The model to check.
            valid_thread_ids: list(str). List of valid thread IDs.
            valid_message_ids: list(tuple(str, int)). List of valid
                (thread_id, message_id) tuples.

        Returns:
            bool. True if the model has any invalid references.
        """
        valid_thread_set: Set[str] = set(valid_thread_ids)
        valid_message_set: Set[Tuple[str, int]] = set(valid_message_ids)

        for ref in model.feedback_message_references:
            thread_id = ref['thread_id']
            message_id = ref['message_id']
            if thread_id not in valid_thread_set:
                return True
            if (thread_id, message_id) not in valid_message_set:
                return True
        return False

    @staticmethod
    def _get_invalid_reference_details(
        model: feedback_models.UnsentFeedbackEmailModel,
        valid_thread_ids: List[str],
        valid_message_ids: List[Tuple[str, int]],
    ) -> List[Tuple[str, str, int, str]]:
        """Gets details of invalid references in the model.

        Args:
            model: UnsentFeedbackEmailModel. The model to check.
            valid_thread_ids: list(str). List of valid thread IDs.
            valid_message_ids: list(tuple(str, int)). List of valid
                (thread_id, message_id) tuples.

        Returns:
            list(tuple(str, str, int, str)). List of tuples containing
                (user_id, thread_id, message_id, reason).
        """
        valid_thread_set: Set[str] = set(valid_thread_ids)
        valid_message_set: Set[Tuple[str, int]] = set(valid_message_ids)
        details: List[Tuple[str, str, int, str]] = []

        for ref in model.feedback_message_references:
            thread_id = ref['thread_id']
            message_id = ref['message_id']
            if thread_id not in valid_thread_set:
                details.append(
                    (model.id, thread_id, message_id, 'thread_not_found')
                )
            elif (thread_id, message_id) not in valid_message_set:
                details.append(
                    (model.id, thread_id, message_id, 'message_not_found')
                )
        return details

    @staticmethod
    def _should_delete_model(
        model: feedback_models.UnsentFeedbackEmailModel,
        valid_thread_ids: List[str],
        valid_message_ids: List[Tuple[str, int]],
    ) -> bool:
        """Checks if the model should be deleted (no valid references remain).

        Args:
            model: UnsentFeedbackEmailModel. The model to check.
            valid_thread_ids: list(str). List of valid thread IDs.
            valid_message_ids: list(tuple(str, int)). List of valid
                (thread_id, message_id) tuples.

        Returns:
            bool. True if the model should be deleted.
        """
        valid_thread_set: Set[str] = set(valid_thread_ids)
        valid_message_set: Set[Tuple[str, int]] = set(valid_message_ids)

        for ref in model.feedback_message_references:
            thread_id = ref['thread_id']
            message_id = ref['message_id']
            if (
                thread_id in valid_thread_set
                and (thread_id, message_id) in valid_message_set
            ):
                return False
        return True

    @staticmethod
    def _clean_invalid_references(
        model: feedback_models.UnsentFeedbackEmailModel,
        valid_thread_ids: List[str],
        valid_message_ids: List[Tuple[str, int]],
    ) -> feedback_models.UnsentFeedbackEmailModel:
        """Removes invalid references from the model.

        Args:
            model: UnsentFeedbackEmailModel. The model to clean.
            valid_thread_ids: list(str). List of valid thread IDs.
            valid_message_ids: list(tuple(str, int)). List of valid
                (thread_id, message_id) tuples.

        Returns:
            UnsentFeedbackEmailModel. The model with invalid references removed.
        """
        valid_thread_set: Set[str] = set(valid_thread_ids)
        valid_message_set: Set[Tuple[str, int]] = set(valid_message_ids)

        valid_refs = [
            ref
            for ref in model.feedback_message_references
            if (
                ref['thread_id'] in valid_thread_set
                and (ref['thread_id'], ref['message_id']) in valid_message_set
            )
        ]
        model.feedback_message_references = valid_refs
        model.update_timestamps()
        return model


class AuditInvalidUnsentFeedbackReferencesJob(
    CleanupInvalidUnsentFeedbackReferencesJob
):
    """Audit job for invalid UnsentFeedbackEmailModel references.

    This job only reports invalid references without modifying the datastore.
    """

    DATASTORE_UPDATES_ALLOWED = False
