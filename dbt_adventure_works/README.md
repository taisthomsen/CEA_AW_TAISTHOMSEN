# Adventure Works dbt Project

This dbt project implements a modern analytics stack for Adventure Works, a bicycle manufacturer, following dimensional modeling principles.

## Project Overview

Adventure Works has over 500 distinct products, 20,000 customers, and 31,000 orders. This project creates a data warehouse to support sales performance analysis and business intelligence.

## Project Structure

```
models/
├── staging/          # Raw data transformations
│   ├── erp/         # ERP system data (SAP)
│   ├── crm/         # CRM system data (SalesForce)
│   ├── web_analytics/ # Web analytics data (Google Analytics)
│   └── website/     # Website data (WordPress)
├── intermediate/     # Business logic transformations
│   └── erp/         # ERP intermediate models
└── marts/           # Final presentation layer
    └── erp/         # ERP mart tables
```

## Data Sources

### ERP (SAP)
- **salesorderheader**: Sales order header information
- **salesorderdetail**: Sales order line items
- **person**: Customer and employee information
- **product**: Product catalog
- **address**: Address information
- **stateprovince**: State/province data
- **countryregion**: Country/region data
- **salesreason**: Sales reason codes

### CRM (SalesForce)
- **customer**: CRM customer information

### Web Analytics (Google Analytics)
- **pageviews**: Website page view data

### Website (WordPress)
- **posts**: Website blog posts

## Dimensional Model

### Fact Table
- **fct_sales**: Sales transactions with key metrics

### Dimension Tables
- **dim_customer**: Customer information
- **dim_product**: Product catalog
- **dim_date**: Date attributes and hierarchies
- **dim_salesperson**: Salesperson information
- **dim_location**: Geographic location data
- **dim_sales_reason**: Sales reason codes

## Key Metrics

As per the data dictionary:
- **Order Count**: Distinct count of salesorderid
- **Quantity Purchased**: Sum of orderqty
- **Total Amount**: Sum of [unitprice * orderqty]
- **Net Amount**: Sum of [unitprice * orderqty * (1-discount)]

## Business Questions Supported

1. Sales performance by product, customer, location, time, salesperson, and sales reason
2. Average ticket analysis by month, year, city, state, and country
3. Top 10 customers by total value transacted
4. Top 5 cities by total value transacted
5. Time series analysis of orders, quantity, and value
6. Product performance for "Promotion" sales reason

## Getting Started

### Prerequisites
- dbt Core or dbt Cloud
- Access to Adventure Works database
- Snowflake account (recommended)

### Installation
1. Clone this repository
1. Install dbt: `pip install -r requirements.txt`
1. Copy the [`.env.example`](.env.example) file as a `.env` file and fill it out
1. Run `source .env` to load the variables
1. Run `dbt deps` to install dependencies
1. Run `dbt debug` to ensure everything is working
1. Run `dbt run` to build all models

### Running the Project
```bash
# Build all models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve

# Run specific models
dbt run --select staging
dbt run --select marts
```

## Testing Strategy

### Source Tests
- Primary key uniqueness
- Required field validation
- Data type validation

### Model Tests
- Referential integrity
- Business logic validation
- Data quality checks

### Custom Tests
- Sales amount validation against accounting figures
- Date range validation
- Business rule compliance

## Data Quality

The project includes comprehensive testing to ensure:
- Data consistency with accounting audit team figures
- Referential integrity across all tables
- Business rule compliance
- Data freshness and completeness

## Contributing

1. Follow the naming convention: `layer_source__model_name`
2. Include comprehensive documentation
3. Add appropriate tests for all models
4. Follow dbt best practices

## License

This project is part of the Adventure Works Analytics Engineering certification.
