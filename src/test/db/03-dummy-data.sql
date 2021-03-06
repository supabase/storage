-- insert users
INSERT INTO "auth"."users" ("instance_id", "id", "aud", "role", "email", "encrypted_password", "confirmed_at", "invited_at", "confirmation_token", "confirmation_sent_at", "recovery_token", "recovery_sent_at", "email_change_token", "email_change", "email_change_sent_at", "last_sign_in_at", "raw_app_meta_data", "raw_user_meta_data", "is_super_admin", "created_at", "updated_at") VALUES
('00000000-0000-0000-0000-000000000000', '317eadce-631a-4429-a0bb-f19a7a517b4a', 'authenticated', 'authenticated', 'inian+user2@supabase.io', '', NULL, '2021-02-17 04:41:13.408828+00', '541rn7rTZPGeGCYsp0a38g', '2021-02-17 04:41:13.408828+00', '', NULL, '', '', NULL, NULL, '{"provider": "email"}', 'null', 'f', '2021-02-17 04:41:13.406912+00', '2021-02-17 04:41:13.406919+00'),
('00000000-0000-0000-0000-000000000000', '4d56e902-f0a0-4662-8448-a4d9e643c142', 'authenticated', 'authenticated', 'inian+user1@supabase.io', '', NULL, '2021-02-17 04:40:58.570482+00', 'U1HvzExEO3l7JzP-4tTxJA', '2021-02-17 04:40:58.570482+00', '', NULL, '', '', NULL, NULL, '{"provider": "email"}', 'null', 'f', '2021-02-17 04:40:58.568637+00', '2021-02-17 04:40:58.568642+00'),
('00000000-0000-0000-0000-000000000000', 'd8c7bce9-cfeb-497b-bd61-e66ce2cbdaa2', 'authenticated', 'authenticated', 'inian+admin@supabase.io', '', NULL, '2021-02-17 04:40:42.901743+00', '3EG99GjT_e3NC4eGEBXOjw', '2021-02-17 04:40:42.901743+00', '', NULL, '', '', NULL, NULL, '{"provider": "email"}', 'null', 'f', '2021-02-17 04:40:42.890632+00', '2021-02-17 04:40:42.890637+00');

-- insert buckets
INSERT INTO "public"."buckets" ("id", "name", "owner", "createdAt", "updatedAt") VALUES
('7078bc23-9dd6-460d-8b93-082254fee63a', 'bucket2', '4d56e902-f0a0-4662-8448-a4d9e643c142', '2021-02-17 04:43:32.770206+00', '2021-02-17 04:43:32.770206+00'),
('7206ba57-513a-4181-971a-feca9ef45862', 'temp1', '317eadce-631a-4429-a0bb-f19a7a517b4a', '2021-02-27 03:04:25.6386+00', '2021-02-27 03:04:25.6386+00'),
('e29843e6-047e-4b1f-9906-20cc06f4aad4', 'bucket4', '317eadce-631a-4429-a0bb-f19a7a517b4a', '2021-02-25 09:23:01.58385+00', '2021-02-25 09:23:01.58385+00');


-- insert objects
INSERT INTO "public"."objects" ("id", "bucketId", "name", "owner", "createdAt", "updatedAt", "lastAccessedAt", "metadata") VALUES
('03e458f9-892f-4db2-8cb9-d3401a689e25', '7078bc23-9dd6-460d-8b93-082254fee63a', 'public/sadcat-upload23.png', '317eadce-631a-4429-a0bb-f19a7a517b4a', '2021-03-04 08:26:08.553748+00', '2021-03-04 08:26:08.553748+00', '2021-03-04 08:26:08.553748+00', '{"mimetype": "image/svg+xml"}'),
('070825af-a11d-44fe-9f1d-abdc76f686f2', '7078bc23-9dd6-460d-8b93-082254fee63a', 'public/sadcat-upload.png', '317eadce-631a-4429-a0bb-f19a7a517b4a', '2021-03-02 16:31:11.115996+00', '2021-03-02 16:31:11.115996+00', '2021-03-02 16:31:11.115996+00', '{"mimetype": "image/png"}'),
('0cac5609-11e1-4f21-b486-d0eeb60909f6', '7078bc23-9dd6-460d-8b93-082254fee63a', 'curlimage.jpg', 'd8c7bce9-cfeb-497b-bd61-e66ce2cbdaa2', '2021-02-23 11:05:16.625075+00', '2021-02-23 11:05:16.625075+00', '2021-02-23 11:05:16.625075+00', NULL),
('147c6795-94d5-4008-9d81-f7ba3b4f8a9f', '7078bc23-9dd6-460d-8b93-082254fee63a', 'folder/only_uid.jpg', 'd8c7bce9-cfeb-497b-bd61-e66ce2cbdaa2', '2021-02-17 10:36:01.504227+00', '2021-02-17 11:03:03.049618+00', '2021-02-17 10:36:01.504227+00', NULL),
('65a3aa9c-0ff2-4adc-85d0-eab673c27443', '7078bc23-9dd6-460d-8b93-082254fee63a', 'authenticated/casestudy.png', 'd8c7bce9-cfeb-497b-bd61-e66ce2cbdaa2', '2021-02-17 10:42:19.366559+00', '2021-02-17 11:03:30.025116+00', '2021-02-17 10:42:19.366559+00', NULL),
('10ABE273-D77A-4BDA-B410-6FC0CA3E6ADC', '7078bc23-9dd6-460d-8b93-082254fee63a', 'authenticated/cat.jpg', 'd8c7bce9-cfeb-497b-bd61-e66ce2cbdaa2', '2021-02-17 10:42:19.366559+00', '2021-02-17 11:03:30.025116+00', '2021-02-17 10:42:19.366559+00', NULL),
('1edccac7-0876-4e9f-89da-a08d2a5f654b', '7078bc23-9dd6-460d-8b93-082254fee63a', 'authenticated/delete.png', '317eadce-631a-4429-a0bb-f19a7a517b4a', '2021-03-02 16:31:11.115996+00', '2021-03-02 16:31:11.115996+00', '2021-03-02 16:31:11.115996+00', '{"mimetype": "image/png"}'),
('1a911f3c-8c1d-4661-93c1-8e065e4d757e', '7078bc23-9dd6-460d-8b93-082254fee63a', 'authenticated/delete1.png', '317eadce-631a-4429-a0bb-f19a7a517b4a', '2021-02-22 22:29:15.14732+00', '2021-02-22 22:29:15.14732+00', '2021-03-02 09:32:17.116+00', '{"mimetype": "image/png"}'),
('372d5d74-e24d-49dc-abe8-47d7eb226a2e', '7078bc23-9dd6-460d-8b93-082254fee63a', 'authenticated/delete-multiple1.png', '317eadce-631a-4429-a0bb-f19a7a517b4a', '2021-02-22 22:29:15.14732+00', '2021-02-22 22:29:15.14732+00', '2021-03-02 09:32:17.116+00', '{"mimetype": "image/png"}'),
('34811c1b-85e5-4eb6-a5e3-d607b2f6986e', '7078bc23-9dd6-460d-8b93-082254fee63a', 'authenticated/delete-multiple2.png', '317eadce-631a-4429-a0bb-f19a7a517b4a', '2021-02-22 22:29:15.14732+00', '2021-02-22 22:29:15.14732+00', '2021-03-02 09:32:17.116+00', '{"mimetype": "image/png"}'),
('45950ff2-d3a8-4add-8e49-bafc01198340', '7078bc23-9dd6-460d-8b93-082254fee63a', 'authenticated/delete-multiple3.png', '317eadce-631a-4429-a0bb-f19a7a517b4a', '2021-02-22 22:29:15.14732+00', '2021-02-22 22:29:15.14732+00', '2021-03-02 09:32:17.116+00', '{"mimetype": "image/png"}'),
('469b0216-5419-41f6-9a37-2abfd7fad29c', '7078bc23-9dd6-460d-8b93-082254fee63a', 'authenticated/delete-multiple4.png', '317eadce-631a-4429-a0bb-f19a7a517b4a', '2021-02-22 22:29:15.14732+00', '2021-02-22 22:29:15.14732+00', '2021-03-02 09:32:17.116+00', '{"mimetype": "image/png"}'),
('55930619-a668-4dbc-aea3-b93dfe101e7f', '7078bc23-9dd6-460d-8b93-082254fee63a', 'authenticated/delete-multiple7.png', '317eadce-631a-4429-a0bb-f19a7a517b4a', '2021-02-22 22:29:15.14732+00', '2021-02-22 22:29:15.14732+00', '2021-03-02 09:32:17.116+00', '{"mimetype": "image/png"}'),
('8377527d-3518-4dc8-8290-c6926470e795', '7078bc23-9dd6-460d-8b93-082254fee63a', 'folder/subfolder/public-all-permissions.png', 'd8c7bce9-cfeb-497b-bd61-e66ce2cbdaa2', '2021-02-17 10:26:42.791214+00', '2021-02-17 11:03:30.025116+00', '2021-02-17 10:26:42.791214+00', NULL),
('b39ae4ab-802b-4c42-9271-3f908c34363c', '7078bc23-9dd6-460d-8b93-082254fee63a', 'private/sadcat-upload3.png', '317eadce-631a-4429-a0bb-f19a7a517b4a', '2021-03-01 08:53:29.567975+00', '2021-03-01 08:53:29.567975+00', '2021-03-01 08:53:29.567975+00', '{"mimetype": "image/svg+xml"}');


-- add policies
CREATE POLICY crud_public_folder ON objects for all USING ("bucketId"='7078bc23-9dd6-460d-8b93-082254fee63a' and (foldername(name))[1] = 'public');
CREATE POLICY crud_public_file ON objects for all USING ("bucketId"='7078bc23-9dd6-460d-8b93-082254fee63a' and name = 'folder/subfolder/public-all-permissions.png');
CREATE POLICY crud_uid_folder ON objects for all USING ("bucketId"='7078bc23-9dd6-460d-8b93-082254fee63a' and (foldername(name))[1] = 'only_uid' and auth.uid() = 'd8c7bce9-cfeb-497b-bd61-e66ce2cbdaa2');
CREATE POLICY crud_uid_file ON objects for all USING ("bucketId"='7078bc23-9dd6-460d-8b93-082254fee63a' and name = 'folder/only_uid.jpg' and auth.uid() = 'd8c7bce9-cfeb-497b-bd61-e66ce2cbdaa2');
CREATE POLICY authenticated_folder ON objects for all USING ("bucketId"='7078bc23-9dd6-460d-8b93-082254fee63a' and (foldername(name))[1] = 'authenticated' and auth.role() = 'authenticated');
CREATE POLICY crud_owner_only ON objects for all USING ("bucketId"='7078bc23-9dd6-460d-8b93-082254fee63a' and (foldername(name))[1] = 'only_owner' and owner = auth.uid());
CREATE POLICY delete_owner_only ON objects for all USING ("bucketId"='7078bc23-9dd6-460d-8b93-082254fee63a' and (foldername(name))[1] = 'only_owner' and owner = auth.uid());
CREATE POLICY open_all_update ON objects for all WITH CHECK ("bucketId"='e29843e6-047e-4b1f-9906-20cc06f4aad4');