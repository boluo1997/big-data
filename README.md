# 数据中台

[![build](https://github.com/Anduin2017/HowToCook/actions/workflows/build.yml/badge.svg)](https://github.com/Anduin2017/HowToCook/actions/workflows/build.yml)
[![License](https://img.shields.io/github/license/Anduin2017/HowToCook)](./LICENSE)
[![GitHub contributors](https://img.shields.io/github/contributors/Anduin2017/HowToCook)](https://github.com/Anduin2017/HowToCook/graphs/contributors)
[![npm](https://img.shields.io/npm/v/how-to-cook)](https://www.npmjs.com/package/how-to-cook)

## data-mock

| time  | schema | table                | enabled |
|-------|--------|----------------------|---------|
| 00:15 | api*   | api_today_on_history | Y       |

## data-batch-ingestion

| time  | schema  | table  | enabled |
|-------|---------|--------|---------|
| 00:30 | bronze* | bronze | N       |

## data-cleansing

| time  | schema  | table  | enabled |
|-------|---------|--------|---------|
| 00:45 | silver* | silver | N       |

---

# TODO

## data-streaming-ingestion

| time | schema | table | enabled |
|------|--------|-------|---------|
| TBD  | TBD    | TBD   | TBD     |