-- Reset existing thumbnails: old rows reference a separate thumbnail bucket
-- that is no longer used. Thumbnails will regenerate on demand in the same bucket.
DELETE FROM "MediaThumbnail";
DELETE FROM "BackgroundTask" WHERE "type" = 'thumbnail_generate' AND "status" = 'pending';
