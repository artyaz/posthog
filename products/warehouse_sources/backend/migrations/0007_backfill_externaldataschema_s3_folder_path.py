from django.db import migrations


def forwards(apps, schema_editor):
    ExternalDataSchema = apps.get_model("warehouse_sources", "ExternalDataSchema")

    qs = ExternalDataSchema.objects.filter(s3_folder_path__isnull=True).only("id", "name", "sync_type_config")
    to_update = []
    for schema in qs.iterator(chunk_size=2000):
        storage_key = (schema.sync_type_config or {}).get("dwh_storage_key")
        # Legacy storage key when present and non-empty, else the standard value: the schema name.
        schema.s3_folder_path = storage_key if isinstance(storage_key, str) and storage_key else schema.name
        to_update.append(schema)
        if len(to_update) >= 2000:
            ExternalDataSchema.objects.bulk_update(to_update, ["s3_folder_path"])
            to_update = []

    if to_update:
        ExternalDataSchema.objects.bulk_update(to_update, ["s3_folder_path"])


class Migration(migrations.Migration):
    dependencies = [
        ("warehouse_sources", "0006_externaldataschema_s3_folder_path"),
    ]

    operations = [
        migrations.RunPython(forwards, migrations.RunPython.noop, elidable=True),
    ]
