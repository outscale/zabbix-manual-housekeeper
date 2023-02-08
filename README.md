# Zabbix Manual Housekeeper :put_litter_in_its_place:
[![Project Sandbox](https://docs.outscale.com/fr/userguide/_images/Project-Sandbox-yellow.svg)](https://docs.outscale.com/en/userguide/Open-Source-Projects.html)

## Description
Zabbix Housekeeper delete old entries without a lines limits and can block others requests for huge monitoring platforms.

Zabbix Manual Housekeeper can delete old entries by batch of X lines.

Tested for Zabbix 4.4.10

## Installation
You will need [Python 3.6+](https://www.python.org/) or later.
It is a good practice to create a dedicated virtualenv first. Even if it usually won't harm to install Python libraries directly on the system, better to contain dependencies in a virtual environment.

- Clone this repository
- Change directory
```
cd zabbix-manual-housekeeper
```
- Create a virtualenv
```
python3 -m venv .venv
source .venv/bin/activate
```
**You may need to install the MySQL development headers and libraries.**

- Install Python packages
```
pip3 install -r requirements.txt
```

## Usage

Fill the settings.py file with your informations about Zabbix database.

```
python3 manual_housekeeper.py --help
Usage: manual_housekeeper.py [options]

Options:
  -h, --help     show this help message and exit
  --node=NODE    SQL node
  --batch=BATCH  How many events to delete by request
  --limit=LIMIT  SQL request limit
  --month=MONTH  How many month to keep
  --only-trends  Delete only trends/trends_uint.
```

| Option | Example |
| ------ | ------ |
| node | 127.0.0.1:3306 |
| batch | 100 |
| limit | 1000 |
| month | 3 |
| only-trends | N/A |

## Contributing
- If you think you've found a bug in the code or you have a question regarding the usage of this software, please reach out to us by opening an issue in this GitHub repository.
- Contributions to this project are welcome: if you want to add a feature or a fix a bug, please do so by opening a Pull Request in this GitHub repository. In case of feature contribution, we kindly ask you to open an issue to discuss it beforehand.

## License
> Copyright Outscale SAS
>
> BSD-3-Clause
