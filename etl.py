import subprocess


def load_bronze():
    subprocess.run(["python", "src/load_bronze.py"], check=True)


def build_silver():
    subprocess.run(["python", "src/build_silver.py"], check=True)


def build_gold():
    subprocess.run(["python", "src/build_gold.py"], check=True)


def all():
    load_bronze()
    build_silver()
    build_gold()


if __name__ == "__main__":
    all()
