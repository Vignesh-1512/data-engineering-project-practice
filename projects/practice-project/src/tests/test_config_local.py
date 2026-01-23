from de_project.utils.config_loader import load_config

def test_config():
    config = load_config()

    dataset = "events"
    process_date = "2026-01-05"

    table_pattern = config["datasets"][dataset]["table_name"]
    table_name = table_pattern.format(process_date=process_date)

    print("Table pattern :", table_pattern)
    print("Final table   :", table_name)

if __name__ == "__main__":
    test_config()
