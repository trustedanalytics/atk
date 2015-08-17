--
-- PostgreSQL Schema Migration
--
-- See FlywayDB.org documentation to see how these changes are applied
--
-- !!! DO NOT MODIFY THIS SCRIPT !!!
--

--
-- update status to have more a more straightforward life-cycle
--


-- if the status is either scheduled for deletion (4), meta data deleted (5) or data deleted(8) set to delete_final
-- otherwise set it to Active
UPDATE frame SET status_id=1 WHERE status_id not in (4, 5, 8);
UPDATE frame SET status_id=3 WHERE status_id in (4, 5, 8);

UPDATE graph SET status_id=1 WHERE status_id not in (4, 5, 8);
UPDATE graph SET status_id=3 WHERE status_id in (4, 5, 8);

UPDATE model SET status_id=1 WHERE status_id not in (4, 5, 8);
UPDATE model SET status_id=3 WHERE status_id in (4, 5, 8);



UPDATE status
    SET name= 'ACTIVE', description = 'Active and can be interacted with'
    WHERE status_id = 1;

UPDATE status
    SET description = 'User has marked as Deleted but can still be un-deleted by interacting with, no action has yet been taken on disk',
     name = 'DELETED'
    WHERE status_id = 2;

UPDATE status
    SET description = 'Underlying storage has been reclaimed, no un-delete is possible',
     name = 'DELETED_FINAL'
    WHERE status_id = 3;


DELETE FROM status where status_id > 3;
