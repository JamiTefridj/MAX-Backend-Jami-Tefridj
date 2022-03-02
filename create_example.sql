CREATE TABLE `ARTIST`  (
	`export_date` BIGINT,
    `artist_id` BIGINT,
    `name` VARCHAR(1000),
	`is_actual_artist` BOOLEAN,
    `view_url` VARCHAR(1000),
    `artist_type_id` INTEGER,
PRIMARY KEY (`artist_id`)
);
        