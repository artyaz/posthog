from django.db import migrations

BATCH_SIZE = 2000


def forwards(apps, schema_editor):
    ExternalDataSchema = apps.get_model("warehouse_sources", "ExternalDataSchema")

    # Updated rows fall out of the NULL filter, so refetching advances the backfill batch by
    # batch and an interrupted run resumes where it stopped.
    while True:
        batch = list(
            ExternalDataSchema.objects.filter(s3_folder_path__isnull=True).only("id", "name", "sync_type_config")[
                :BATCH_SIZE
            ]
        )
        if not batch:
            break
        for schema in batch:
            storage_key = (schema.sync_type_config or {}).get("dwh_storage_key")
            # Legacy storage key when present and non-empty, else the standard value: the schema name.
            schema.s3_folder_path = storage_key if isinstance(storage_key, str) and storage_key else schema.name
        ExternalDataSchema.objects.bulk_update(batch, ["s3_folder_path"])


class Migration(migrations.Migration):
    # Each batch commits on its own; the NULL filter makes a rerun resume where it left off.
    atomic = False

    dependencies = [
        ("warehouse_sources", "0006_externaldataschema_s3_folder_path"),
    ]

    operations = [
        migrations.RunPython(forwards, migrations.RunPython.noop, elidable=True),
    ]
