# COSMIC-DEMO-AWS

This repo contains all the DAGs and other necessary bits to support the demo environment described in the [Astro Demo Environment](https://docs.google.com/document/d/1VD8q-VFHybnrV4eWHVtUAsk-jkj8eNfviTU6UxJP9Sw/edit#heading=h.sjuvr1u9rrx4) document.

### General Demo Guidelines

1. All demo should strive to be immutable and require **zero** setup.  Demo-ers should be able to login and begin there demo with no pre-work. 
2. The end goal is for only a subset of individuals to have anyhting beyond Viewer permissions in this Cosmic Energy Organization, and for *every* Astronomer employee to have read-only access.

### Dag Todo List 
- [x] Main Dag
- [x] AWS Services dag
- [ ] Broken DAG (observability)
- [ ] Broken DAG (lineage)
- [x] Astro SDK Dag 
- [x] Lineage DAG
- [x] Machine Learning DAG