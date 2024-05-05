## XOOG Candidate evaluation task resolution

This repository contains resolution of recruitment task
for **Junior Python Programmer** Position at XOOG.

Task description was provided by XOOG and placed in unmodified [docx file](./task/Zadanie%20rekrutacyjne.docx).

## Requirements

- Python =< 3.11

NOTE: Older versions were not tested but should work as intended.

## Environment setup

Create new *venv* python virtual environment in [this repository](./)

```
python3 -m venv venv
```

Activate environment

```
source ./venv/bin/activate
```

Install required 3rd party libraries from [requirements.txt](./requirements.txt)

```
python3 -m pip -U install pip
python3 -m pip install -r requirements.txt
```

## Authorization

To work with this repository you need valid JAO API KEY (token).

You can get new token immediately on your email using [this link](https://www.jao.eu/get-token).

Paste valid token into file [.JAO_API_KEY](.JAO_API_KEY) in this repository. 

Alternatively, you can pass API KEY as argument `-k` when executing script 

API KEY should look like this:
`7zz8f0uu-g7r7-618a-haaa-ha7jna642a9m`. 
**NOTE: this is not a valid API KEY.**

## Usage

After each session remember to use correct virtual environment

```
source venv/bin/activate
```

Run the script

```
python3 scraper.py
```

Navigate to [results/JOINED.csv](./results/JOINED.csv) for final report.

You can investigate interim files in [downloads](./downloads/) dir.

For additional info refer to help with `-h` argument:

```
python3 scraper.py -h
```

## Development

This repository uses unittest. 
Each contribution should be preceded by check:

```
python3 -m unittest
```
