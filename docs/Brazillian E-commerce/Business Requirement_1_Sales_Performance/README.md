# BR-1: Sales & Performance

## 1. Business Objective
The goal of this business requirement is to understand overall sales performance by analyzing
revenue, order volume, product demand, and sales trends over time.

---

## 2. Business Questions Answered
- What is the total revenue?
- How many orders are placed daily or monthly?
- What is the average order value (AOV)?
- Which product categories generate the most revenue?
- How does sales vary over time?

---

## 3. Why the Business Cares
- Measure company revenue growth
- Identify top-performing product categories
- Detect seasonality and sales trends
- Support financial and revenue reporting
- Enable data-driven business decisions

---

## 4. Source Tables (Gold Layer)

| Table Name | Purpose |
|-----------|--------|
| brazillian_e_commerce.gold.fact_sales | Revenue and quantity metrics |
| brazillian_e_commerce.gold.fact_orders | Order counts |
| brazillian_e_commerce.gold.dim_date | Time-based analysis |
| brazillian_e_commerce.gold.dim_products | Product and category details |
| brazillian_e_commerce.gold.dim_customers | Customer segmentation |

---

## 5. Final Output Table

**Layer:** Final  
**Table Name:** sales_performance  


---

## 6. Grain
**One row per DAY per PRODUCT CATEGORY**

This grain ensures:
- Correct aggregation
- No data duplication
- Easy rollups for monthly or quarterly analysis

---

## 7. Output Schema

| Column Name | Description |
|------------|------------|
| sales_date | Date of sale |
| product_category | Product category |
| total_revenue | Total revenue |
| total_orders | Total number of orders |
| total_quantity_sold | Total quantity sold |
| avg_order_value | Average order value |

---

## 8. Conceptual Build Logic


---

## 6. Grain
**One row per DAY per PRODUCT CATEGORY**

This grain ensures:
- Correct aggregation
- No data duplication
- Easy rollups for monthly or quarterly analysis

---

## 7. Output Schema

| Column Name | Description |
|------------|------------|
| sales_date | Date of sale |
| product_category | Product category |
| total_revenue | Total revenue |
| total_orders | Total number of orders |
| total_quantity_sold | Total quantity sold |
| avg_order_value | Average order value |

---

## 8. Conceptual Build Logic

fact_sales
JOIN dim_products
JOIN dim_date
JOIN fact_orders
→ GROUP BY sales_date, product_category
→ AGGREGATE metrics
→ STORE in olist.final.sales_performance

---

## 9. Notes
- This is a derived aggregate table
- Core fact tables remain the single source of truth
- Designed for BI tools and reporting consumption