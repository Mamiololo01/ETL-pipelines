# ETL Pipelines

This repository contains different ETL (Extract, Transform, Load) pipeline projects. Each project demonstrates various aspects of data engineering, including data extraction, transformation, and loading using different tools and platforms.

## Projects

### 1. [API_ETL](https://github.com/Mamiololo01/ETL-pipelines/tree/main/API_ETL)
This project contains the pipeline for extracting exchange rates data from XE API, transforming the data, and loading it to a PostgreSQL database.

### 2. [Azure_Databricks_ETL](https://github.com/Mamiololo01/ETL-pipelines/tree/main/Azure_Databricks_ETL)
This project creates an end-to-end data pipeline that moves data from an on-prem SQL database to the Azure cloud, following steps such as Data Ingestion, Data Transformation, Data Loading, Data Governance, and Data Analytics.

#### Project Goals:
- Data Ingestion: Extract data from on-prem SQL Server Database using Azure Data Factory.
- Data Storage: Store data in Azure Data Lake Gen 2 storage.
- Data Transformation: Perform ETL jobs using Azure Databricks.
- Data Governance: Use Azure Key Vaults and Active Directory.
- Data Analytics: Integrate with Power BI using Azure Synapse Analytics.

### 3. [Covid19_data](https://github.com/Mamiololo01/ETL-pipelines/tree/main/Covid19_data)
This project focuses on ETL pipelines related to COVID-19 data.

## How to Contribute

We welcome contributions to the ETL Pipelines repository. Here are the steps to get started:

1. **Fork the Repository**:
   - Navigate to the repository [ETL-pipelines](https://github.com/Mamiololo01/ETL-pipelines) and click on the "Fork" button in the top-right corner to create a copy of the repository in your GitHub account.

2. **Clone the Forked Repository**:
   - Clone the forked repository to your local machine using the following command:
     ```sh
     git clone https://github.com/your-username/ETL-pipelines.git
     cd ETL-pipelines
     ```

3. **Create a New Branch**:
   - Create a new branch to work on your changes:
     ```sh
     git checkout -b my-feature
     ```

4. **Make Your Changes**:
   - Make the necessary changes or additions to the repository code. Ensure your code follows the existing code style and includes necessary documentation.

5. **Commit and Push Your Changes**:
   - Commit your changes with a descriptive message:
     ```sh
     git add .
     git commit -m "Description of the changes"
     git push origin my-feature
     ```

6. **Create a Pull Request**:
   - Go to your forked repository on GitHub, and you should see a "Compare & pull request" button. Click on it to create a pull request to the original repository.

7. **Address Review Comments**:
   - Engage with the maintainers, address any review comments, and make necessary changes until your pull request is approved and merged.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

Feel free to open an issue or contact the maintainers if you have any questions or need assistance.

Happy coding!
