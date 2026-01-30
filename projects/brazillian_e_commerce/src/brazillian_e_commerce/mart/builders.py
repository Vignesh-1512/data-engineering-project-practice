

from brazillian_e_commerce.mart.transform_sales_performance import build_sales_performance
from brazillian_e_commerce.mart.transform_order_delivery_summary import build_order_delivery_summary
from brazillian_e_commerce.mart.transform_customer_analytics import build_customer_analytics
from brazillian_e_commerce.mart.transform_seller_performance import build_seller_performance
from brazillian_e_commerce.mart.transform_product_category_performance import build_product_category_performance
from brazillian_e_commerce.mart.transform_payment_analytics import build_payment_analytics
from brazillian_e_commerce.mart.transform_customer_satisfaction_and_reviews import build_review_satisfaction

BUILDERS = {
    "sales_performance": build_sales_performance,          # BR-1
    "order_delivery_summary": build_order_delivery_summary,# BR-2
    "customer_analytics": build_customer_analytics,        # BR-3
    "seller_performance": build_seller_performance,         # BR-4
    "product_category_performance": build_product_category_performance, #BR-5
    "payment_analytics" : build_payment_analytics,       # BR-6
    "customer_satisfaction_and_reviews" : build_review_satisfaction #BR-7
}
