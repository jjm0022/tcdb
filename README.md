<div
    align="center"
>

# TCDB 

A tropical cyclone database pipeline

<!-- Badges -->
![GitHub language count](https://img.shields.io/github/languages/count/jjm0022/tcdb?style=plastic)
![GitHub last commit](https://img.shields.io/github/last-commit/jjm0022/tcdb)
![GitHub issues](https://img.shields.io/github/issues/jjm0022/tcdb)
![GitHub pull requests](https://img.shields.io/github/issues-pr/jjm0022/tcdb)
<!-- (Badges) -->
</div>

## Environment Variables
The environment vairiables needed to run some bash scripts are sourced from `.env`. Just copy the [`.env_example`](https://github.com/jjm0022/tcdb/blob/main/.env_example) and edit the variables:

```bash
cp .env_example .env
```

## Secrets File
The file `.secrets.yml` is needed to access the DB. Just copy the `secrets_example.yml` file and edit the variables:

```bash
cp .secrets_example.yml .secrets.yml
```