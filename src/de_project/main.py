from de_project.jobs.extract_job import extract_run
from de_project.jobs.transform_job import transform_run

def main(dataset=None, process_date=None, job=None):
    import sys
    if dataset is None or process_date is None or job is None:
        if len(sys.argv) != 4:
            print("Usage: python -m de_project.main <dataset> <process_date> <job>")
            print("job = extract | transform | extract_transform")
            sys.exit(1)

        dataset = sys.argv[1]
        process_date = sys.argv[2]
        job=sys.argv[3]

    if job == "extract":
        extract_run(dataset, process_date)

    elif job == "transform":
        transform_run(dataset, process_date)

    elif job == "extract_transform":
        extract_run(dataset, process_date)
        transform_run(dataset, process_date)

    else:
        raise ValueError(
            f"Invalid job type: {job}. "
            "Use extract | transform | extract_transform"
        )

if __name__ == "__main__":
    main()
