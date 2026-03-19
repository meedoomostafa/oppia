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

"""Unit tests for unsent_feedback_email_audit_jobs."""

from __future__ import annotations

from core.jobs import job_test_utils
from core.jobs.batch_jobs import unsent_feedback_email_audit_jobs
from core.jobs.types import job_run_result
from core.platform import models

from typing import Type

MYPY = False
if MYPY:  # pragma: no cover
    from mypy_imports import feedback_models

(feedback_models,) = models.Registry.import_models([models.Names.FEEDBACK])


class AuditInvalidUnsentFeedbackReferencesJobTest(job_test_utils.JobTestBase):
    """Tests for AuditInvalidUnsentFeedbackReferencesJob."""

    JOB_CLASS: Type[
        unsent_feedback_email_audit_jobs.AuditInvalidUnsentFeedbackReferencesJob
    ] = unsent_feedback_email_audit_jobs.AuditInvalidUnsentFeedbackReferencesJob

    def test_empty_datastore(self) -> None:
        """Test that the job produces no output when there are no models."""
        self.assert_job_output_is([])

    def test_valid_references_produce_no_output(self) -> None:
        """Test that valid references are not reported as invalid."""
        thread = self.create_model(
            feedback_models.GeneralFeedbackThreadModel,
            id='exploration.exp1.thread1',
            entity_type='exploration',
            entity_id='exp1',
            status='open',
            subject='subject',
            message_count=1,
            has_suggestion=False,
            deleted=False,
        )

        message = self.create_model(
            feedback_models.GeneralFeedbackMessageModel,
            id='exploration.exp1.thread1.0',
            thread_id='exploration.exp1.thread1',
            message_id=0,
            author_id='user1',
            text='feedback text',
            received_via_email=False,
            deleted=False,
        )

        unsent_email = self.create_model(
            feedback_models.UnsentFeedbackEmailModel,
            id='owner1',
            feedback_message_references=[
                {
                    'entity_type': 'exploration',
                    'entity_id': 'exp1',
                    'thread_id': 'exploration.exp1.thread1',
                    'message_id': 0,
                }
            ],
            retries=0,
            deleted=False,
        )

        self.put_multi([thread, message, unsent_email])

        self.assert_job_output_is([])

    def test_reference_to_nonexistent_thread_is_reported(self) -> None:
        """Test that references to non-existent threads are reported."""
        unsent_email = self.create_model(
            feedback_models.UnsentFeedbackEmailModel,
            id='owner1',
            feedback_message_references=[
                {
                    'entity_type': 'exploration',
                    'entity_id': 'exp1',
                    'thread_id': 'exploration.exp1.missing_thread',
                    'message_id': 0,
                }
            ],
            retries=0,
            deleted=False,
        )

        self.put_multi([unsent_email])

        self.assert_job_output_is(
            [
                job_run_result.JobRunResult.as_stdout(
                    (
                        'Invalid reference in UnsentFeedbackEmailModel: '
                        'user_id=owner1, '
                        'thread_id=exploration.exp1.missing_thread, '
                        'message_id=0, '
                        'reason=thread_not_found'
                    )
                ),
                job_run_result.JobRunResult.as_stdout(
                    'invalid_references_count: 1'
                ),
                job_run_result.JobRunResult.as_stdout(
                    'affected_unsent_email_models_count: 1'
                ),
            ]
        )

    def test_reference_to_nonexistent_message_is_reported(self) -> None:
        """Test that references to non-existent messages are reported."""
        # Thread exists but message does not.
        thread = self.create_model(
            feedback_models.GeneralFeedbackThreadModel,
            id='exploration.exp1.thread1',
            entity_type='exploration',
            entity_id='exp1',
            status='open',
            subject='subject',
            message_count=1,
            has_suggestion=False,
            deleted=False,
        )

        unsent_email = self.create_model(
            feedback_models.UnsentFeedbackEmailModel,
            id='owner1',
            feedback_message_references=[
                {
                    'entity_type': 'exploration',
                    'entity_id': 'exp1',
                    'thread_id': 'exploration.exp1.thread1',
                    'message_id': 99,  # Non-existent message.
                }
            ],
            retries=0,
            deleted=False,
        )

        self.put_multi([thread, unsent_email])

        self.assert_job_output_is(
            [
                job_run_result.JobRunResult.as_stdout(
                    (
                        'Invalid reference in UnsentFeedbackEmailModel: '
                        'user_id=owner1, '
                        'thread_id=exploration.exp1.thread1, '
                        'message_id=99, '
                        'reason=message_not_found'
                    )
                ),
                job_run_result.JobRunResult.as_stdout(
                    'invalid_references_count: 1'
                ),
                job_run_result.JobRunResult.as_stdout(
                    'affected_unsent_email_models_count: 1'
                ),
            ]
        )

    def test_mixed_valid_and_invalid_references(self) -> None:
        """Test that only invalid references are reported."""
        thread = self.create_model(
            feedback_models.GeneralFeedbackThreadModel,
            id='exploration.exp1.thread1',
            entity_type='exploration',
            entity_id='exp1',
            status='open',
            subject='subject',
            message_count=1,
            has_suggestion=False,
            deleted=False,
        )

        message = self.create_model(
            feedback_models.GeneralFeedbackMessageModel,
            id='exploration.exp1.thread1.0',
            thread_id='exploration.exp1.thread1',
            message_id=0,
            author_id='user1',
            text='feedback text',
            received_via_email=False,
            deleted=False,
        )

        # Has one valid and one invalid reference.
        unsent_email = self.create_model(
            feedback_models.UnsentFeedbackEmailModel,
            id='owner1',
            feedback_message_references=[
                {
                    'entity_type': 'exploration',
                    'entity_id': 'exp1',
                    'thread_id': 'exploration.exp1.thread1',
                    'message_id': 0,
                },
                {
                    'entity_type': 'exploration',
                    'entity_id': 'exp1',
                    'thread_id': 'exploration.exp1.missing_thread',
                    'message_id': 0,
                },
            ],
            retries=0,
            deleted=False,
        )

        self.put_multi([thread, message, unsent_email])

        self.assert_job_output_is(
            [
                job_run_result.JobRunResult.as_stdout(
                    (
                        'Invalid reference in UnsentFeedbackEmailModel: '
                        'user_id=owner1, '
                        'thread_id=exploration.exp1.missing_thread, '
                        'message_id=0, '
                        'reason=thread_not_found'
                    )
                ),
                job_run_result.JobRunResult.as_stdout(
                    'invalid_references_count: 1'
                ),
                job_run_result.JobRunResult.as_stdout(
                    'affected_unsent_email_models_count: 1'
                ),
            ]
        )


class CleanupInvalidUnsentFeedbackReferencesJobTest(job_test_utils.JobTestBase):
    """Tests for CleanupInvalidUnsentFeedbackReferencesJob."""

    JOB_CLASS: Type[
        unsent_feedback_email_audit_jobs.CleanupInvalidUnsentFeedbackReferencesJob
    ] = (
        unsent_feedback_email_audit_jobs.CleanupInvalidUnsentFeedbackReferencesJob
    )

    def test_empty_datastore(self) -> None:
        """Test that the job produces no output when there are no models."""
        self.assert_job_output_is([])

    def test_valid_references_are_not_modified(self) -> None:
        """Test that models with only valid references are not modified."""
        thread = self.create_model(
            feedback_models.GeneralFeedbackThreadModel,
            id='exploration.exp1.thread1',
            entity_type='exploration',
            entity_id='exp1',
            status='open',
            subject='subject',
            message_count=1,
            has_suggestion=False,
            deleted=False,
        )

        message = self.create_model(
            feedback_models.GeneralFeedbackMessageModel,
            id='exploration.exp1.thread1.0',
            thread_id='exploration.exp1.thread1',
            message_id=0,
            author_id='user1',
            text='feedback text',
            received_via_email=False,
            deleted=False,
        )

        unsent_email = self.create_model(
            feedback_models.UnsentFeedbackEmailModel,
            id='owner1',
            feedback_message_references=[
                {
                    'entity_type': 'exploration',
                    'entity_id': 'exp1',
                    'thread_id': 'exploration.exp1.thread1',
                    'message_id': 0,
                }
            ],
            retries=0,
            deleted=False,
        )

        self.put_multi([thread, message, unsent_email])

        self.assert_job_output_is([])

        # Verify the model was not modified.
        model = feedback_models.UnsentFeedbackEmailModel.get('owner1')
        self.assertEqual(len(model.feedback_message_references), 1)

    def test_model_with_all_invalid_refs_is_deleted(self) -> None:
        """Test that models with all invalid references are deleted."""
        unsent_email = self.create_model(
            feedback_models.UnsentFeedbackEmailModel,
            id='owner1',
            feedback_message_references=[
                {
                    'entity_type': 'exploration',
                    'entity_id': 'exp1',
                    'thread_id': 'exploration.exp1.missing_thread',
                    'message_id': 0,
                }
            ],
            retries=0,
            deleted=False,
        )

        self.put_multi([unsent_email])

        self.assert_job_output_is(
            [
                job_run_result.JobRunResult.as_stdout(
                    (
                        'Deleted UnsentFeedbackEmailModel (no valid refs): '
                        'user_id=owner1'
                    )
                ),
                job_run_result.JobRunResult.as_stdout(
                    'deleted_unsent_email_models_count: 1'
                ),
            ]
        )

        # Verify the model was deleted.
        model = feedback_models.UnsentFeedbackEmailModel.get(
            'owner1', strict=False
        )
        self.assertIsNone(model)

    def test_model_with_mixed_refs_is_updated(self) -> None:
        """Test that models with mixed valid/invalid refs are updated."""
        thread = self.create_model(
            feedback_models.GeneralFeedbackThreadModel,
            id='exploration.exp1.thread1',
            entity_type='exploration',
            entity_id='exp1',
            status='open',
            subject='subject',
            message_count=1,
            has_suggestion=False,
            deleted=False,
        )

        message = self.create_model(
            feedback_models.GeneralFeedbackMessageModel,
            id='exploration.exp1.thread1.0',
            thread_id='exploration.exp1.thread1',
            message_id=0,
            author_id='user1',
            text='feedback text',
            received_via_email=False,
            deleted=False,
        )

        # Has one valid and one invalid reference.
        unsent_email = self.create_model(
            feedback_models.UnsentFeedbackEmailModel,
            id='owner1',
            feedback_message_references=[
                {
                    'entity_type': 'exploration',
                    'entity_id': 'exp1',
                    'thread_id': 'exploration.exp1.thread1',
                    'message_id': 0,
                },
                {
                    'entity_type': 'exploration',
                    'entity_id': 'exp1',
                    'thread_id': 'exploration.exp1.missing_thread',
                    'message_id': 0,
                },
            ],
            retries=0,
            deleted=False,
        )

        self.put_multi([thread, message, unsent_email])

        self.assert_job_output_is(
            [
                job_run_result.JobRunResult.as_stdout(
                    (
                        'Updated UnsentFeedbackEmailModel (removed invalid refs): '
                        'user_id=owner1'
                    )
                ),
                job_run_result.JobRunResult.as_stdout(
                    'updated_unsent_email_models_count: 1'
                ),
            ]
        )

        # Verify the model was updated with only the valid reference.
        model = feedback_models.UnsentFeedbackEmailModel.get('owner1')
        self.assertEqual(len(model.feedback_message_references), 1)
        self.assertEqual(
            model.feedback_message_references[0]['thread_id'],
            'exploration.exp1.thread1',
        )

    def test_multiple_models_with_different_issues(self) -> None:
        """Test handling multiple models with various issues."""
        thread = self.create_model(
            feedback_models.GeneralFeedbackThreadModel,
            id='exploration.exp1.thread1',
            entity_type='exploration',
            entity_id='exp1',
            status='open',
            subject='subject',
            message_count=1,
            has_suggestion=False,
            deleted=False,
        )

        message = self.create_model(
            feedback_models.GeneralFeedbackMessageModel,
            id='exploration.exp1.thread1.0',
            thread_id='exploration.exp1.thread1',
            message_id=0,
            author_id='user1',
            text='feedback text',
            received_via_email=False,
            deleted=False,
        )

        # Model to be deleted (all invalid).
        unsent_email_to_delete = self.create_model(
            feedback_models.UnsentFeedbackEmailModel,
            id='owner_delete',
            feedback_message_references=[
                {
                    'entity_type': 'exploration',
                    'entity_id': 'exp1',
                    'thread_id': 'exploration.exp1.missing',
                    'message_id': 0,
                }
            ],
            retries=0,
            deleted=False,
        )

        # Model to be updated (mixed).
        unsent_email_to_update = self.create_model(
            feedback_models.UnsentFeedbackEmailModel,
            id='owner_update',
            feedback_message_references=[
                {
                    'entity_type': 'exploration',
                    'entity_id': 'exp1',
                    'thread_id': 'exploration.exp1.thread1',
                    'message_id': 0,
                },
                {
                    'entity_type': 'exploration',
                    'entity_id': 'exp1',
                    'thread_id': 'exploration.exp1.also_missing',
                    'message_id': 0,
                },
            ],
            retries=0,
            deleted=False,
        )

        # Model with all valid references (should not be touched).
        unsent_email_valid = self.create_model(
            feedback_models.UnsentFeedbackEmailModel,
            id='owner_valid',
            feedback_message_references=[
                {
                    'entity_type': 'exploration',
                    'entity_id': 'exp1',
                    'thread_id': 'exploration.exp1.thread1',
                    'message_id': 0,
                }
            ],
            retries=0,
            deleted=False,
        )

        self.put_multi(
            [
                thread,
                message,
                unsent_email_to_delete,
                unsent_email_to_update,
                unsent_email_valid,
            ]
        )

        self.assert_job_output_is(
            [
                job_run_result.JobRunResult.as_stdout(
                    (
                        'Deleted UnsentFeedbackEmailModel (no valid refs): '
                        'user_id=owner_delete'
                    )
                ),
                job_run_result.JobRunResult.as_stdout(
                    'deleted_unsent_email_models_count: 1'
                ),
                job_run_result.JobRunResult.as_stdout(
                    (
                        'Updated UnsentFeedbackEmailModel (removed invalid refs): '
                        'user_id=owner_update'
                    )
                ),
                job_run_result.JobRunResult.as_stdout(
                    'updated_unsent_email_models_count: 1'
                ),
            ]
        )

        # Verify the deleted model is gone.
        self.assertIsNone(
            feedback_models.UnsentFeedbackEmailModel.get(
                'owner_delete', strict=False
            )
        )

        # Verify the updated model has only the valid reference.
        updated_model = feedback_models.UnsentFeedbackEmailModel.get(
            'owner_update'
        )
        self.assertEqual(len(updated_model.feedback_message_references), 1)

        # Verify the valid model is unchanged.
        valid_model = feedback_models.UnsentFeedbackEmailModel.get(
            'owner_valid'
        )
        self.assertEqual(len(valid_model.feedback_message_references), 1)
