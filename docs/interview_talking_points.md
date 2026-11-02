# Interview talking points

Use these to describe the project honestly: it is a **personal portfolio project built on
synthetic, realistic data**, not a production system at an employer. Say that upfront; the
engineering decisions are what interviewers care about.

---

**"Walk me through a pipeline you built end to end."**
> I built a Permian Basin operations platform on Microsoft Fabric and Azure Data Factory. ADF pulls
> four sources: work orders and parts inventory incrementally from a SQL Server ERP using a
> metadata-driven watermark pattern, SCADA production and pump-sensor files from an ADLS landing zone,
> and WTI and Henry Hub prices from the EIA API. Everything lands in a Fabric Lakehouse bronze layer.
> ADF then triggers a Fabric pipeline through the Fabric REST API, which runs PySpark notebooks for
> bronze-to-silver cleansing and silver-to-gold business logic, loads a star schema in the Fabric
> Warehouse with stored procedures, checks that warehouse totals reconcile to the lakehouse, and only
> then refreshes a Direct Lake Power BI model. I designed and built all of it, including the data
> generator, tests and runbook.

**"How much Fabric / ADF?"**
> ADF: linked services with Key Vault, a self-hosted IR source, parameterised datasets, Lookup /
> ForEach / GetMetadata / Filter / Until / Fail activities, REST pagination, and a single failure-alert
> path. Fabric: Lakehouse with Delta, notebooks with parameters and resources, a Warehouse, Fabric
> pipelines, and a Direct Lake semantic model. *(Be clear this is project experience if it isn't from a job yet.)*

**"How do you use SQL?"**
> The Warehouse layer is all T-SQL: SCD Type 2 with hash-based change detection, point-in-time surrogate
> key lookups, delete-and-insert fact loads in a transaction, TRY/CATCH with audit logging and THROW,
> and reconciliation views. I also had to work within Fabric Warehouse limits: no enforced keys, no
> DEFAULT constraints.

**"How do you use Python?"**
> The transformations live in a plain PySpark module separate from the notebooks so I can unit-test them
> locally with pytest. It includes a small data-quality rule framework, de-duplication of restated
> records, window functions for deferred production and rolling baselines, and a z-score health model.
> I also wrote the synthetic data generator using an Arps decline curve so production looks realistic.

**"Tell me about a data model you designed."**
> Four facts at clear grains: production per well per day, pump health per pump per day, work orders,
> and inventory snapshots, sharing date, well, equipment, part, warehouse and failure-mode dimensions.
> Well is SCD2 so a change in operator doesn't rewrite history. The business problem: tying lost
> production to pump failures and to whether the parts were on the shelf. On the sample data, about
> $3.1 MM of revenue was deferred, and 77 % of "Critical" pump flags preceded a failure within a week.

**"Have you supported production pipelines?"**
> I designed this for it: every step is idempotent so you can just re-run, watermarks only advance after
> a successful copy, bad rows go to a quarantine table instead of disappearing, a 5 % circuit breaker
> stops bad data from reaching reports, and there's a runbook for the common failures.
> *(Add your real production-support examples from HPE or UH here.)*

**Why this fits DXP specifically**
> DXP supplies pumps, MRO parts and supply-chain services to oil and gas operators. The pump-health and
> parts days-of-cover pieces of this project are the same questions a distributor's customers ask:
> which equipment is failing and is the replacement in stock.
