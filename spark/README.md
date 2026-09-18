# Spark Java module

Module Maven cho các job Silver và Gold sẽ được triển khai tại đây.

```text
spark/
└── src/
    ├── main/java/              # Java production source
    └── test/
        ├── java/               # Unit tests
        └── resources/fixtures/ # Fixture riêng cho Spark tests
```

`SPK-01` chịu trách nhiệm thêm Maven Wrapper, `pom.xml`, package namespace,
Hello World và test base. Không commit `target/`, JAR hoặc local metastore.

Trong giai đoạn scaffold, dùng lệnh ở project root:

```bash
./scripts/check-repository-layout.sh
```

