package com.hand;

import com.hand.config.Config;
import com.hand.config.TopicConfig;
import com.hand.producer.SpringUtils;
import com.hand.producer.kafkaProducer;
import com.hand.redis.*;
import net.sf.json.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.*;
import java.util.logging.Logger;

public class ConsumerThread extends Thread {
    private static final Logger logger = Logger.getLogger(ConsumerThread.class.getName());
    private Config config;
    private TopicConfig topicConfig;
    private KafkaConsumer<String, String> consumer;
    private Boolean running = false;

    kafkaProducer kafkaproduce = new kafkaProducer();

    AlipayDao alipayDao = (AlipayDao) SpringUtils.getBean("alipayDao");
    AreaDao areaDao = (AreaDao) SpringUtils.getBean("areaDao");
    AttributionOnlineDao attributionOnlineDao = (AttributionOnlineDao) SpringUtils.getBean("attributionOnlineDao");
    AttributionRelationOnlineDao attributionRelationOnlineDao = (AttributionRelationOnlineDao) SpringUtils.getBean("attributionRelationOnlineDao");
    AttributionRelationStagedDao attributionRelationStagedDao = (AttributionRelationStagedDao) SpringUtils.getBean("attributionRelationStagedDao");
    AttributionStagedDao attributionStagedDao = (AttributionStagedDao) SpringUtils.getBean("attributionStagedDao");
    BaseStoreDao baseStoreDao = (BaseStoreDao) SpringUtils.getBean("baseStoreDao");
    CaptchaDao captchaDao = (CaptchaDao) SpringUtils.getBean("captchaDao");
    CartDao cartDao = (CartDao) SpringUtils.getBean("cartDao");
    CategoryOnlineDao categoryOnlineDao = (CategoryOnlineDao) SpringUtils.getBean("categoryOnlineDao");
    CategoryRelationOnlineDao categoryRelationOnlineDao = (CategoryRelationOnlineDao) SpringUtils.getBean("categoryRelationOnlineDao");
    CategoryRelationStagedDao categoryRelationStagedDao = (CategoryRelationStagedDao) SpringUtils.getBean("categoryRelationStagedDao");
    CategoryStagedDao categoryStagedDao = (CategoryStagedDao) SpringUtils.getBean("categoryStagedDao");
    CMSSiteDao cmsSiteDao = (CMSSiteDao) SpringUtils.getBean("cmsSiteDao");
    CouponDao couponDao = (CouponDao) SpringUtils.getBean("couponDao");
    CustomerCouponDao customerCouponDao = (CustomerCouponDao) SpringUtils.getBean("customerCouponDao");
    DeliveryTemplateDao deliveryTemplateDao = (DeliveryTemplateDao) SpringUtils.getBean("deliveryTemplateDao");
    EmailPathDao emailPathDao = (EmailPathDao) SpringUtils.getBean("emailPathDao");
    ErrorDao errorDao = (ErrorDao) SpringUtils.getBean("errorDao");
    EvaluationDao evaluationDao = (EvaluationDao) SpringUtils.getBean("evaluationDao");
    InStockStatusDao inStockStatusDao = (InStockStatusDao) SpringUtils.getBean("inStockStatusDao");
    LoginDao loginDao = (LoginDao) SpringUtils.getBean("loginDao");
    LogisticsCompaniesDao logisticsCompaniesDao = (LogisticsCompaniesDao) SpringUtils.getBean("logisticsCompaniesDao");
    MobileDao mobileDao = (MobileDao) SpringUtils.getBean("mobileDao");
    OrderDao orderDao = (OrderDao) SpringUtils.getBean("orderDao");
    OrderExpressDao orderExpressDao = (OrderExpressDao) SpringUtils.getBean("orderExpressDao");
    OrderNodeDao orderNodeDao = (OrderNodeDao) SpringUtils.getBean("orderNodeDao");
    OrderPickupCodeDao orderPickupCodeDao = (OrderPickupCodeDao) SpringUtils.getBean("orderPickupCodeDao");
    OrderPickupDao orderPickupDao = (OrderPickupDao) SpringUtils.getBean("orderPickupDao");
    OrderPriceDao orderPriceDao = (OrderPriceDao) SpringUtils.getBean("orderPriceDao");
    OrderProductDao orderProductDao = (OrderProductDao) SpringUtils.getBean("orderProductDao");
    OrderStatusDao orderStatusDao = (OrderStatusDao) SpringUtils.getBean("orderStatusDao");
    OrderTempDao orderTempDao = (OrderTempDao) SpringUtils.getBean("orderTempDao");
    PaymentModeDao paymentModeDao = (PaymentModeDao) SpringUtils.getBean("paymentModeDao");
    PointOfServiceDao pointOfServiceDao = (PointOfServiceDao) SpringUtils.getBean("pointOfServiceDao");
    ProductDetailOnlineDao productDetailOnlineDao = (ProductDetailOnlineDao) SpringUtils.getBean("productDetailOnlineDao");
    ProductDetailStagedDao productDetailStagedDao = (ProductDetailStagedDao) SpringUtils.getBean("productDetailStagedDao");
    ProductRecommendOnlineDao productRecommendOnlineDao = (ProductRecommendOnlineDao) SpringUtils.getBean("productRecommendOnlineDao");
    ProductRecommendStagedDao productRecommendStagedDao = (ProductRecommendStagedDao) SpringUtils.getBean("productRecommendStagedDao");
    ProductResourceOnlineDao productResourceOnlineDao = (ProductResourceOnlineDao) SpringUtils.getBean("productResourceOnlineDao");
    ProductResourceStagedDao productResourceStagedDao = (ProductResourceStagedDao) SpringUtils.getBean("productResourceStagedDao");
    ProductSummaryOnlineDao productSummaryOnlineDao = (ProductSummaryOnlineDao) SpringUtils.getBean("productSummaryOnlineDao");
    ProductSummaryStagedDao productSummaryStagedDao = (ProductSummaryStagedDao) SpringUtils.getBean("productSummaryStagedDao");
    PromptDao promptDao = (PromptDao) SpringUtils.getBean("promptDao");
    PurchaseAdviceDao purchaseAdviceDao = (PurchaseAdviceDao) SpringUtils.getBean("purchaseAdviceDao");
    PwdErrorDao pwdErrorDao = (PwdErrorDao) SpringUtils.getBean("pwdErrorDao");
    QQDao qqDao = (QQDao) SpringUtils.getBean("qqDao");
    QuickLoginDao quickLoginDao = (QuickLoginDao) SpringUtils.getBean("quickLoginDao");
    RefundDao refundDao = (RefundDao) SpringUtils.getBean("refundDao");
    RulesActionDao rulesActionDao = (RulesActionDao) SpringUtils.getBean("rulesActionDao");
    RulesConditionDao rulesConditionDao = (RulesConditionDao) SpringUtils.getBean("rulesConditionDao");
    RulesModelDao rulesModelDao = (RulesModelDao) SpringUtils.getBean("rulesModelDao");
    RulesTmplDao rulesTmplDao = (RulesTmplDao) SpringUtils.getBean("rulesTmplDao");
    SaleActivityDao saleActivityDao = (SaleActivityDao) SpringUtils.getBean("saleActivityDao");
    SaleDiscountCouponDao saleDiscountCouponDao = (SaleDiscountCouponDao) SpringUtils.getBean("saleDiscountCouponDao");
    SaleTemplateDao saleTemplateDao = (SaleTemplateDao) SpringUtils.getBean("saleTemplateDao");
    ShippingReplacementDao shippingReplacementDao = (ShippingReplacementDao) SpringUtils.getBean("shippingReplacementDao");
    SiteMsgDao siteMsgDao = (SiteMsgDao) SpringUtils.getBean("siteMsgDao");
    SiteMsgReadDao siteMsgReadDao = (SiteMsgReadDao) SpringUtils.getBean("siteMsgReadDao");
    SiteMsgTypeDao siteMsgTypeDao = (SiteMsgTypeDao) SpringUtils.getBean("siteMsgTypeDao");
    SliderCaptchaDao sliderCaptchaDao = (SliderCaptchaDao) SpringUtils.getBean("sliderCaptchaDao");
    SmsDao smsDao = (SmsDao) SpringUtils.getBean("smsDao");
    StaffDao staffDao = (StaffDao) SpringUtils.getBean("staffDao");
    StaffMobileDao staffMobileDao = (StaffMobileDao) SpringUtils.getBean("staffMobileDao");
    StockCenterDao stockCenterDao = (StockCenterDao) SpringUtils.getBean("stockCenterDao");
    StockDao stockDao = (StockDao) SpringUtils.getBean("stockDao");
    StockStoreDao stockStoreDao = (StockStoreDao) SpringUtils.getBean("stockStoreDao");
    TaoAreaDao taoAreaDao = (TaoAreaDao) SpringUtils.getBean("taoAreaDao");
    TaobaoDao taobaoDao = (TaobaoDao) SpringUtils.getBean("taobaoDao");
    TestDao testDao = (TestDao) SpringUtils.getBean("testDao");
    ThirdPartyDao thirdPartyDao = (ThirdPartyDao) SpringUtils.getBean("thirdPartyDao");
    UserAddressDao userAddressDao = (UserAddressDao) SpringUtils.getBean("userAddressDao");
    UserDao userDao = (UserDao) SpringUtils.getBean("userDao");
    UserFavoriteDao userFavoriteDao = (UserFavoriteDao) SpringUtils.getBean("userFavoriteDao");
    UserInfoDao userInfoDao = (UserInfoDao) SpringUtils.getBean("userInfoDao");
    WeiboDao weiboDao = (WeiboDao) SpringUtils.getBean("weiboDao");
    WeixiDao weixiDao = (WeixiDao) SpringUtils.getBean("weixiDao");
    ZoneDeliveryModeDao zoneDeliveryModeDao = (ZoneDeliveryModeDao) SpringUtils.getBean("zoneDeliveryModeDao");
    ZoneDeliveryModeValueDao zoneDeliveryModeValueDao = (ZoneDeliveryModeValueDao) SpringUtils.getBean("zoneDeliveryModeValueDao");


    public ConsumerThread(Config config, TopicConfig topicConfig) throws Exception {
        try {
            this.config = config;
            this.topicConfig = topicConfig;
            Properties props = new Properties();
            props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getGroupId());
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServer());
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
            consumer = new KafkaConsumer<String, String>(props);
            consumer.subscribe(Collections.singletonList(topicConfig.getTopic()));
            running = true;
        } catch (Exception e) {
            if (consumer != null) {
                consumer.close();
            }
            throw e;
        }
    }

    @Override
    public void run() {
        Thread.currentThread().setName("ConsumerThread-" + topicConfig.getTopic());
        try {
            while (running) {

                ConsumerRecords<String, String> records = consumer.poll(1000);
                logger.info(String.format("poll count:" + records.count()));
                HashMap<String, Object> staff1 = new HashMap<>();
                for (ConsumerRecord<String, String> record : records) {
                    String key = record.key();
                    String value = record.value();
                    System.out.println(record);
                    if (key != null && value != null) {
                        String[] names = value.split("\\|");
                        for (int i = 0; i < names.length; i++) ;
                        if (names[0].equals("add")) {
                            HashMap<String, String> map = new HashMap<String, String>();
                            // 将json字符串转换成jsonObject
                            JSONObject jsonObject = JSONObject.fromObject(names[2]);
                            Iterator it = jsonObject.keys();
                            // 遍历jsonObject数据，添加到Map对象
                            while (it.hasNext()) {
                                String key1 = String.valueOf(it.next());
                                String value1 = jsonObject.get(key1).toString();
                                map.put(key1, value1);
                            }
                            switch (names[1]) {
                                case "hmall:cache:{thirdParty}thirdParty:alipay":
                                    Map<String, Objects> recalipay = (Map<String, Objects>) alipayDao.selectRecycle(map.get("alipayOpenId"));
                                    Map<String, Objects> alipay = (Map<String, Objects>) alipayDao.select(map.get("alipayOpenId"));
                                    if (recalipay == null) {
                                        if (alipay == null) {
                                            try {
                                                alipayDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recalipay.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                alipayDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

//                                case "hmall:cache:{base}Base:area":
//                                    break;

                                case "hmall:online:{product}Attr":
                                    Map<String, Objects> recattributiononline = (Map<String, Objects>) attributionOnlineDao.selectRecycle(map.get("attrId"));
                                    Map<String, Objects> attributiononline = (Map<String, Objects>) attributionOnlineDao.select(map.get("attrId"));
                                    if (recattributiononline == null) {
                                        if (attributiononline == null) {
                                            try {
                                                attributionOnlineDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recattributiononline.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                attributionOnlineDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:online:{product}Attr:product":
                                    Map<String, Objects> recattributionrelationonline = (Map<String, Objects>) attributionRelationOnlineDao.selectRecycle(map.get("uid"));
                                    Map<String, Objects> attributionrelationonline = (Map<String, Objects>) attributionRelationOnlineDao.select(map.get("uid"));
                                    if (recattributionrelationonline == null) {
                                        if (attributionrelationonline == null) {
                                            try {
                                                attributionRelationOnlineDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recattributionrelationonline.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                attributionRelationOnlineDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:staged:{product}Attr:product":
                                    Map<String, Objects> recattributionrelationstaged = (Map<String, Objects>) attributionRelationStagedDao.selectRecycle(map.get("uid"));
                                    Map<String, Objects> attributionrelationstaged = (Map<String, Objects>) attributionRelationStagedDao.select(map.get("uid"));
                                    if (recattributionrelationstaged == null) {
                                        if (attributionrelationstaged == null) {
                                            try {
                                                attributionRelationStagedDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recattributionrelationstaged.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                attributionRelationStagedDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:staged:{product}Attr":
                                    Map<String, Objects> recattributionstaged = (Map<String, Objects>) attributionStagedDao.selectRecycle(map.get("attrId"));
                                    Map<String, Objects> attributionstaged = (Map<String, Objects>) attributionStagedDao.select(map.get("attrId"));
                                    if (recattributionstaged == null) {
                                        if (attributionstaged == null) {
                                            try {
                                                attributionStagedDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recattributionstaged.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                attributionStagedDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{Base}Base:baseStore":
                                    Map<String, Objects> recbasestore = (Map<String, Objects>) baseStoreDao.selectRecycle(map.get("storeId"));
                                    Map<String, Objects> basestore = (Map<String, Objects>) baseStoreDao.select(map.get("storeId"));
                                    if (recbasestore == null) {
                                        if (basestore == null) {
                                            try {
                                                baseStoreDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recbasestore.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                baseStoreDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{verification}Captcha":
//                                    Map<String, Objects> reccaptcha = (Map<String, Objects>) captchaDao.selectRecycle(map.get("attrId"));
//                                    Map<String, Objects> captcha = (Map<String, Objects>) captchaDao.select(map.get("attrId"));
//                                    if (reccaptcha == null) {
//                                        if (captcha == null) {
//                                            try {
//                                                captchaDao.add(map);
//                                            } catch (Exception e) {
//                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
//                                                System.out.println("数据库操作失败，转发kafka");
//                                            }
//                                        } else {
//                                            System.out.println("过时数据，不进行操作。");
//                                        }
//                                    } else {
//                                        if (Integer.parseInt(String.valueOf(reccaptcha.get("version"))) > Integer.parseInt(map.get("version"))) {
//                                            System.out.println("过时数据，不进行操作。");
//                                        } else {
//                                            try {
//                                                captchaDao.add(map);
//                                            } catch (Exception e) {
//                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
//                                                System.out.println("数据库操作失败，转发kafka");
//                                            }
//                                        }
//                                    }
                                    break;

                                case "hmall:cache:{cart}Cart":
                                    Map<String, Objects> reccart = (Map<String, Objects>) cartDao.selectRecycle(map.get("cartId"));
                                    Map<String, Objects> cart = (Map<String, Objects>) cartDao.select(map.get("cartId"));
                                    if (reccart == null) {
                                        if (cart == null) {
                                            try {
                                                cartDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(reccart.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                cartDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:online:{product}Category":
                                    Map<String, Objects> reccategoryonline = (Map<String, Objects>) categoryOnlineDao.selectRecycle(map.get("uid"));
                                    Map<String, Objects> categoryonline = (Map<String, Objects>) categoryOnlineDao.select(map.get("uid"));
                                    if (reccategoryonline == null) {
                                        if (categoryonline == null) {
                                            try {
                                                categoryOnlineDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(reccategoryonline.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                categoryOnlineDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:online:{product}Category:product":
                                    Map<String, Objects> reccategoryrelationonline = (Map<String, Objects>) categoryRelationOnlineDao.selectRecycle(map.get("uid"));
                                    Map<String, Objects> categoryrelationonline = (Map<String, Objects>) categoryRelationOnlineDao.select(map.get("uid"));
                                    if (reccategoryrelationonline == null) {
                                        if (categoryrelationonline == null) {
                                            try {
                                                categoryRelationOnlineDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(reccategoryrelationonline.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                categoryRelationOnlineDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:staged:{product}Category:product":
                                    Map<String, Objects> reccategoryrelationstaged = (Map<String, Objects>) categoryRelationStagedDao.selectRecycle(map.get("uid"));
                                    Map<String, Objects> categoryrelationstaged = (Map<String, Objects>) categoryRelationStagedDao.select(map.get("uid"));
                                    if (reccategoryrelationstaged == null) {
                                        if (categoryrelationstaged == null) {
                                            try {
                                                categoryRelationStagedDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(reccategoryrelationstaged.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                categoryRelationStagedDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:staged:{product}Category":
                                    Map<String, Objects> reccategorystaged = (Map<String, Objects>) categoryStagedDao.selectRecycle(map.get("uid"));
                                    Map<String, Objects> categorystaged = (Map<String, Objects>) categoryStagedDao.select(map.get("uid"));
                                    if (reccategorystaged == null) {
                                        if (categorystaged == null) {
                                            try {
                                                categoryStagedDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(reccategorystaged.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                categoryStagedDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{Base}Base:cMSSite":
                                    Map<String, Objects> reccmssite = (Map<String, Objects>) cmsSiteDao.selectRecycle(map.get("cmmsId"));
                                    Map<String, Objects> cmssite = (Map<String, Objects>) cmsSiteDao.select(map.get("cmmsId"));
                                    if (reccmssite == null) {
                                        if (cmssite == null) {
                                            try {
                                                cmsSiteDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(reccmssite.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                cmsSiteDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{promotion}coupon":
                                    Map<String, Objects> reccoupon = (Map<String, Objects>) couponDao.selectRecycle(map.get("couponCode"));
                                    Map<String, Objects> coupon = (Map<String, Objects>) couponDao.select(map.get("couponCode"));
                                    if (reccoupon == null) {
                                        if (coupon == null) {
                                            try {
                                                couponDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(reccoupon.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                couponDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{promotion}coupon:customerCoupon":
                                    Map<String, Objects> reccustomercoupon = (Map<String, Objects>) customerCouponDao.selectRecycle(map.get("couponId"));
                                    Map<String, Objects> customercoupon = (Map<String, Objects>) customerCouponDao.select(map.get("couponId"));
                                    if (reccustomercoupon == null) {
                                        if (customercoupon == null) {
                                            try {
                                                customerCouponDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(reccustomercoupon.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                customerCouponDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{base}Base:deliveryTemplate":
                                    Map<String, Objects> recdeliverytemplate = (Map<String, Objects>) deliveryTemplateDao.selectRecycle(map.get("deliveryTemplateId"));
                                    Map<String, Objects> deliverytemplate = (Map<String, Objects>) deliveryTemplateDao.select(map.get("deliveryTemplateId"));
                                    if (recdeliverytemplate == null) {
                                        if (deliverytemplate == null) {
                                            try {
                                                deliveryTemplateDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recdeliverytemplate.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                deliveryTemplateDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{user}User:emailpath":
                                    Map<String, Objects> recemailpath = (Map<String, Objects>) emailPathDao.selectRecycle(map.get("uid"));
                                    Map<String, Objects> emailpath = (Map<String, Objects>) emailPathDao.select(map.get("uid"));
                                    if (recemailpath == null) {
                                        if (emailpath == null) {
                                            try {
                                                emailPathDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recemailpath.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                emailPathDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{sms}Sms:error":
                                    Map<String, Objects> recerror = (Map<String, Objects>) errorDao.selectRecycle(map.get("uid"));
                                    Map<String, Objects> error = (Map<String, Objects>) errorDao.select(map.get("uid"));
                                    if (recerror == null) {
                                        if (error == null) {
                                            try {
                                                errorDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recerror.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                errorDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{comment}Comment:evaluation":
                                    Map<String, Objects> recevaluation = (Map<String, Objects>) evaluationDao.selectRecycle(map.get("uid"));
                                    Map<String, Objects> evaluation = (Map<String, Objects>) evaluationDao.select(map.get("uid"));
                                    if (recevaluation == null) {
                                        if (evaluation == null) {
                                            try {
                                                evaluationDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recevaluation.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                evaluationDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{Base}Base:inStockStatus":
                                    Map<String, Objects> recinstockstatus = (Map<String, Objects>) inStockStatusDao.selectRecycle(map.get("inStockCode"));
                                    Map<String, Objects> instockstatus = (Map<String, Objects>) inStockStatusDao.select(map.get("inStockCode"));
                                    if (recinstockstatus == null) {
                                        if (instockstatus == null) {
                                            try {
                                                inStockStatusDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recinstockstatus.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                inStockStatusDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{user}User:login":
                                    Map<String, Objects> reclogin = (Map<String, Objects>) loginDao.selectRecycle(map.get("customerId"));
                                    Map<String, Objects> login = (Map<String, Objects>) loginDao.select(map.get("customerId"));
                                    if (reclogin == null) {
                                        if (login == null) {
                                            try {
                                                loginDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(reclogin.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                loginDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{base}Base:logisticsCompaniesDao":
                                    Map<String, Objects> reclogisticscompanies = (Map<String, Objects>) logisticsCompaniesDao.selectRecycle(map.get("code"));
                                    Map<String, Objects> logisticscompanies = (Map<String, Objects>) logisticsCompaniesDao.select(map.get("code"));
                                    if (reclogisticscompanies == null) {
                                        if (logisticscompanies == null) {
                                            try {
                                                logisticsCompaniesDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(reclogisticscompanies.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                logisticsCompaniesDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{customer}Customer:mobile":
                                    Map<String, Objects> recmobile = (Map<String, Objects>) mobileDao.selectRecycle(map.get("mobileNumber"));
                                    Map<String, Objects> mobile = (Map<String, Objects>) mobileDao.select(map.get("mobileNumber"));
                                    if (recmobile == null) {
                                        if (mobile == null) {
                                            try {
                                                mobileDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recmobile.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                mobileDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{order}Order":
                                    Map<String, Objects> recorder = (Map<String, Objects>) orderDao.selectRecycle(map.get("orderId"));
                                    Map<String, Objects> order = (Map<String, Objects>) orderDao.select(map.get("orderId"));
                                    if (recorder == null) {
                                        if (order == null) {
                                            try {
                                                orderDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recorder.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                orderDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{order}Express":
                                    Map<String, Objects> recorderexpress = (Map<String, Objects>) orderExpressDao.selectRecycle(map.get("expressId"));
                                    Map<String, Objects> orderexpress = (Map<String, Objects>) orderExpressDao.select(map.get("expressId"));
                                    if (recorderexpress == null) {
                                        if (orderexpress == null) {
                                            try {
                                                orderExpressDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recorderexpress.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                orderExpressDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{order}Order:node":
                                    Map<String, Objects> recordernode = (Map<String, Objects>) orderNodeDao.selectRecycle(map.get("uid"));
                                    Map<String, Objects> ordernode = (Map<String, Objects>) orderNodeDao.select(map.get("uid"));
                                    if (recordernode == null) {
                                        if (ordernode == null) {
                                            try {
                                                orderNodeDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recordernode.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                orderNodeDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{order}Pickup:code":
                                    Map<String, Objects> recorderpickupcode = (Map<String, Objects>) orderPickupCodeDao.selectRecycle(map.get("uid"));
                                    Map<String, Objects> orderpickupcode = (Map<String, Objects>) orderPickupCodeDao.select(map.get("uid"));
                                    if (recorderpickupcode == null) {
                                        if (orderpickupcode == null) {
                                            try {
                                                orderPickupCodeDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recorderpickupcode.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                orderPickupCodeDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{order}Pickup":
                                    Map<String, Objects> recorderpickup = (Map<String, Objects>) orderPickupDao.selectRecycle(map.get("pickupId"));
                                    Map<String, Objects> orderpickup = (Map<String, Objects>) orderPickupDao.select(map.get("pickupId"));
                                    if (recorderpickup == null) {
                                        if (orderpickup == null) {
                                            try {
                                                orderPickupDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recorderpickup.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                orderPickupDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{order}Order:price":
                                    Map<String, Objects> recorderprice = (Map<String, Objects>) orderPriceDao.selectRecycle(map.get("orderId"));
                                    Map<String, Objects> orderprice = (Map<String, Objects>) orderPriceDao.select(map.get("orderId"));
                                    if (recorderprice == null) {
                                        if (orderprice == null) {
                                            try {
                                                orderPriceDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recorderprice.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                orderPriceDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{order}Order:details":
                                    Map<String, Objects> recorderproduct = (Map<String, Objects>) orderProductDao.selectRecycle(map.get("uid"));
                                    Map<String, Objects> orderproduct = (Map<String, Objects>) orderProductDao.select(map.get("uid"));
                                    if (recorderproduct == null) {
                                        if (orderproduct == null) {
                                            try {
                                                orderProductDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recorderproduct.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                orderProductDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{order}Order:status":
                                    Map<String, Objects> recorderstatus = (Map<String, Objects>) orderStatusDao.selectRecycle(map.get("orderId"));
                                    Map<String, Objects> orderstatus = (Map<String, Objects>) orderStatusDao.select(map.get("orderId"));
                                    if (recorderstatus == null) {
                                        if (orderstatus == null) {
                                            try {
                                                orderStatusDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recorderstatus.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                orderStatusDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{order}Order:temp":
                                    Map<String, Objects> recordertemp = (Map<String, Objects>) orderTempDao.selectRecycle(map.get("tempId"));
                                    Map<String, Objects> ordertemp = (Map<String, Objects>) orderTempDao.select(map.get("tempId"));
                                    if (recordertemp == null) {
                                        if (ordertemp == null) {
                                            try {
                                                orderTempDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recordertemp.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                orderTempDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{base}Base:paymentMode":
                                    Map<String, Objects> recpaymentmode = (Map<String, Objects>) paymentModeDao.selectRecycle(map.get("paymentModeId"));
                                    Map<String, Objects> paymentmode = (Map<String, Objects>) paymentModeDao.select(map.get("paymentModeId"));
                                    if (recpaymentmode == null) {
                                        if (paymentmode == null) {
                                            try {
                                                paymentModeDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recpaymentmode.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                paymentModeDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{base}Base:pointOfService":
                                    Map<String, Objects> recpointofservice = (Map<String, Objects>) pointOfServiceDao.selectRecycle(map.get("code"));
                                    Map<String, Objects> pointofservice = (Map<String, Objects>) pointOfServiceDao.select(map.get("code"));
                                    if (recpointofservice == null) {
                                        if (pointofservice == null) {
                                            try {
                                                pointOfServiceDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recpointofservice.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                pointOfServiceDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:online:{product}Product":
                                    Map<String, Objects> recproductdetailonline = (Map<String, Objects>) productDetailOnlineDao.selectRecycle(map.get("productId"));
                                    Map<String, Objects> productdetailonline = (Map<String, Objects>) productDetailOnlineDao.select(map.get("productId"));
                                    if (recproductdetailonline == null) {
                                        if (productdetailonline == null) {
                                            try {
                                                productDetailOnlineDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recproductdetailonline.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                productDetailOnlineDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:staged:{product}Product":
                                    Map<String, Objects> recroductdetailstaged = (Map<String, Objects>) productDetailStagedDao.selectRecycle(map.get("productId"));
                                    Map<String, Objects> roductdetailstaged = (Map<String, Objects>) productDetailStagedDao.select(map.get("productId"));
                                    if (recroductdetailstaged == null) {
                                        if (roductdetailstaged == null) {
                                            try {
                                                productDetailStagedDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recroductdetailstaged.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                productDetailStagedDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:online:{product}Product:recommend":
                                    Map<String, Objects> recproductrecommendonline = (Map<String, Objects>) productRecommendOnlineDao.selectRecycle(map.get("uid"));
                                    Map<String, Objects> productrecommendonline = (Map<String, Objects>) productRecommendOnlineDao.select(map.get("uid"));
                                    if (recproductrecommendonline == null) {
                                        if (productrecommendonline == null) {
                                            try {
                                                productRecommendOnlineDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recproductrecommendonline.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                productRecommendOnlineDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:staged:{product}Product:recommend":
                                    Map<String, Objects> recproductrecommendstaged = (Map<String, Objects>) productRecommendStagedDao.selectRecycle(map.get("uid"));
                                    Map<String, Objects> productrecommendstaged = (Map<String, Objects>) productRecommendStagedDao.select(map.get("uid"));
                                    if (recproductrecommendstaged == null) {
                                        if (productrecommendstaged == null) {
                                            try {
                                                productRecommendStagedDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recproductrecommendstaged.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                productRecommendStagedDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:online:{product}Product:resource":
                                    Map<String, Objects> recproductresourceonline = (Map<String, Objects>) productResourceOnlineDao.selectRecycle(map.get("productCode"));
                                    Map<String, Objects> productresourceonline = (Map<String, Objects>) productResourceOnlineDao.select(map.get("productCode"));
                                    if (recproductresourceonline == null) {
                                        if (productresourceonline == null) {
                                            try {
                                                productResourceOnlineDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recproductresourceonline.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                productResourceOnlineDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:staged:{product}Product:resource":
                                    Map<String, Objects> recproductresourcestaged = (Map<String, Objects>) productResourceStagedDao.selectRecycle(map.get("productCode"));
                                    Map<String, Objects> productresourcestaged = (Map<String, Objects>) productResourceStagedDao.select(map.get("productCode"));
                                    if (recproductresourcestaged == null) {
                                        if (productresourcestaged == null) {
                                            try {
                                                productResourceStagedDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recproductresourcestaged.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                productResourceStagedDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:online:{product}Product:summary":
                                    Map<String, Objects> recproductsummaryonline = (Map<String, Objects>) productSummaryOnlineDao.selectRecycle(map.get("productCode"));
                                    Map<String, Objects> productsummaryonline = (Map<String, Objects>) productSummaryOnlineDao.select(map.get("productCode"));
                                    if (recproductsummaryonline == null) {
                                        if (productsummaryonline == null) {
                                            try {
                                                productSummaryOnlineDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recproductsummaryonline.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                productSummaryOnlineDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:staged:{product}Product:summary":
                                    Map<String, Objects> recproductsummarystaged = (Map<String, Objects>) productSummaryStagedDao.selectRecycle(map.get("productCode"));
                                    Map<String, Objects> productsummarystaged = (Map<String, Objects>) productSummaryStagedDao.select(map.get("productCode"));
                                    if (recproductsummarystaged == null) {
                                        if (productsummarystaged == null) {
                                            try {
                                                productSummaryStagedDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recproductsummarystaged.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                productSummaryStagedDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{prompt}Prompt:zh":
                                    Map<String, Objects> recprompt = (Map<String, Objects>) promptDao.selectRecycle(map.get("msgCode"));
                                    Map<String, Objects> prompt = (Map<String, Objects>) promptDao.select(map.get("msgCode"));
                                    if (recprompt == null) {
                                        if (prompt == null) {
                                            try {
                                                promptDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recprompt.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                promptDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{comment}Comment:purchaseAdvice":
                                    Map<String, Objects> recpurchaseadvice = (Map<String, Objects>) purchaseAdviceDao.selectRecycle(map.get("uid"));
                                    Map<String, Objects> purchaseadvice = (Map<String, Objects>) purchaseAdviceDao.select(map.get("uid"));
                                    if (recpurchaseadvice == null) {
                                        if (purchaseadvice == null) {
                                            try {
                                                purchaseAdviceDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recpurchaseadvice.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                purchaseAdviceDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{user}User:pwderror":
                                    Map<String, Objects> recpwderror = (Map<String, Objects>) pwdErrorDao.selectRecycle(map.get("uid"));
                                    Map<String, Objects> pwderror = (Map<String, Objects>) pwdErrorDao.select(map.get("uid"));
                                    if (recpwderror == null) {
                                        if (pwderror == null) {
                                            try {
                                                pwdErrorDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recpwderror.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                pwdErrorDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{thirdParty}thirdParty:qq":
                                    Map<String, Objects> reqq = (Map<String, Objects>) qqDao.selectRecycle(map.get("qqOpenId"));
                                    Map<String, Objects> qq = (Map<String, Objects>) qqDao.select(map.get("qqOpenId"));
                                    if (reqq == null) {
                                        if (qq == null) {
                                            try {
                                                qqDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(reqq.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                qqDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{user}User:quicklogin":
                                    Map<String, Objects> recquicklogin = (Map<String, Objects>) quickLoginDao.selectRecycle(map.get("uid"));
                                    Map<String, Objects> quicklogin = (Map<String, Objects>) quickLoginDao.select(map.get("uid"));
                                    if (recquicklogin == null) {
                                        if (quicklogin == null) {
                                            try {
                                                quickLoginDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recquicklogin.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                quickLoginDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{refund}Refund":
                                    Map<String, Objects> recrefund = (Map<String, Objects>) refundDao.selectRecycle(map.get("refundId"));
                                    Map<String, Objects> refund = (Map<String, Objects>) refundDao.select(map.get("refundId"));
                                    if (recrefund == null) {
                                        if (refund == null) {
                                            try {
                                                refundDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recrefund.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                refundDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{rules}Rules:action":
                                    Map<String, Objects> recrulesaction = (Map<String, Objects>) rulesActionDao.selectRecycle(map.get("actionId"));
                                    Map<String, Objects> rulesaction = (Map<String, Objects>) rulesActionDao.select(map.get("actionId"));
                                    if (recrulesaction == null) {
                                        if (rulesaction == null) {
                                            try {
                                                rulesActionDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recrulesaction.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                rulesActionDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{rules}Rules:condition":
                                    Map<String, Objects> recrulescondition = (Map<String, Objects>) rulesConditionDao.selectRecycle(map.get("conditionId"));
                                    Map<String, Objects> rulescondition = (Map<String, Objects>) rulesConditionDao.select(map.get("conditionId"));
                                    if (recrulescondition == null) {
                                        if (rulescondition == null) {
                                            try {
                                                rulesConditionDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recrulescondition.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                rulesConditionDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{rules}Rules:models":
                                    Map<String, Objects> recrulesmode = (Map<String, Objects>) rulesModelDao.selectRecycle(map.get("modelId"));
                                    Map<String, Objects> rulesmode = (Map<String, Objects>) rulesModelDao.select(map.get("modelId"));
                                    if (recrulesmode == null) {
                                        if (rulesmode == null) {
                                            try {
                                                rulesModelDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recrulesmode.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                rulesModelDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{rules}Rules:temp":
                                    Map<String, Objects> recrulestmpl = (Map<String, Objects>) rulesTmplDao.selectRecycle(map.get("tmplId"));
                                    Map<String, Objects> rulestmpl = (Map<String, Objects>) rulesTmplDao.select(map.get("tmplId"));
                                    if (recrulestmpl == null) {
                                        if (rulestmpl == null) {
                                            try {
                                                rulesTmplDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recrulestmpl.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                rulesTmplDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{SaleActivity}SaleActivity":
                                    Map<String, Objects> recsaleactivity = (Map<String, Objects>) saleActivityDao.selectRecycle(map.get("id"));
                                    Map<String, Objects> saleactivity = (Map<String, Objects>) saleActivityDao.select(map.get("id"));
                                    if (recsaleactivity == null) {
                                        if (saleactivity == null) {
                                            try {
                                                saleActivityDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recsaleactivity.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                saleActivityDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{SaleDiscountCoupon}SaleDiscountCoupon":
                                    Map<String, Objects> recsalediscountcoupon = (Map<String, Objects>) saleDiscountCouponDao.selectRecycle(map.get("id"));
                                    Map<String, Objects> salediscountcoupon = (Map<String, Objects>) saleDiscountCouponDao.select(map.get("id"));
                                    if (recsalediscountcoupon == null) {
                                        if (salediscountcoupon == null) {
                                            try {
                                                saleDiscountCouponDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recsalediscountcoupon.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                saleDiscountCouponDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{SaleTemplate}SaleTemplate":
                                    Map<String, Objects> recsaletemplate = (Map<String, Objects>) saleTemplateDao.selectRecycle(map.get("id"));
                                    Map<String, Objects> saletemplate = (Map<String, Objects>) saleTemplateDao.select(map.get("id"));
                                    if (recsaletemplate == null) {
                                        if (saletemplate == null) {
                                            try {
                                                saleTemplateDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recsaletemplate.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                saleTemplateDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{replace}Replace:shippingReplacement":
                                    Map<String, Objects> recshippingreplacement = (Map<String, Objects>) shippingReplacementDao.selectRecycle(map.get("replaceId"));
                                    Map<String, Objects> shippingreplacement = (Map<String, Objects>) shippingReplacementDao.select(map.get("replaceId"));
                                    if (recshippingreplacement == null) {
                                        if (shippingreplacement == null) {
                                            try {
                                                shippingReplacementDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recshippingreplacement.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                shippingReplacementDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{siteMessage}siteMessage":
                                    Map<String, Objects> recsitemsg = (Map<String, Objects>) siteMsgDao.selectRecycle(map.get("uid"));
                                    Map<String, Objects> sitemsg = (Map<String, Objects>) siteMsgDao.select(map.get("uid"));
                                    if (recsitemsg == null) {
                                        if (sitemsg == null) {
                                            try {
                                                siteMsgDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recsitemsg.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                siteMsgDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{siteMessage}siteMessage:read":
                                    Map<String, Objects> recsitemsgread = (Map<String, Objects>) siteMsgReadDao.selectRecycle(map.get("uid"));
                                    Map<String, Objects> sitemsgread = (Map<String, Objects>) siteMsgReadDao.select(map.get("uid"));
                                    if (recsitemsgread == null) {
                                        if (sitemsgread == null) {
                                            try {
                                                siteMsgReadDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recsitemsgread.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                siteMsgReadDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{siteMessage}siteMessage:type":
                                    Map<String, Objects> recsitemsgtype = (Map<String, Objects>) siteMsgTypeDao.selectRecycle(map.get("msgCode"));
                                    Map<String, Objects> sitemsgtype = (Map<String, Objects>) siteMsgTypeDao.select(map.get("msgCode"));
                                    if (recsitemsgtype == null) {
                                        if (sitemsgtype == null) {
                                            try {
                                                siteMsgTypeDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recsitemsgtype.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                siteMsgTypeDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{verification}SliderCaptcha":
//                                    Map<String, Objects> recattributionstaged = (Map<String, Objects>) sliderCaptchaDao.selectRecycle(map.get("attrId"));
//                                    Map<String, Objects> attributionstaged = (Map<String, Objects>) sliderCaptchaDao.select(map.get("attrId"));
//                                    if (recattributionstaged == null) {
//                                        if (attributionstaged == null) {
//                                            try {
//                                                sliderCaptchaDao.add(map);
//                                            } catch (Exception e) {
//                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
//                                                System.out.println("数据库操作失败，转发kafka");
//                                            }
//                                        } else {
//                                            System.out.println("过时数据，不进行操作。");
//                                        }
//                                    } else {
//                                        if (Integer.parseInt(String.valueOf(recattributionstaged.get("version"))) > Integer.parseInt(map.get("version"))) {
//                                            System.out.println("过时数据，不进行操作。");
//                                        } else {
//                                            try {
//                                                sliderCaptchaDao.add(map);
//                                            } catch (Exception e) {
//                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
//                                                System.out.println("数据库操作失败，转发kafka");
//                                            }
//                                        }
//                                    }
                                    break;

                                case "hmall:cache:{sms}Sms":
                                    Map<String, Objects> recsms = (Map<String, Objects>) smsDao.selectRecycle(map.get("uid"));
                                    Map<String, Objects> sms = (Map<String, Objects>) smsDao.select(map.get("uid"));
                                    if (recsms == null) {
                                        if (sms == null) {
                                            try {
                                                smsDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recsms.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                smsDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{base}Base:staff":
                                    Map<String, Objects> recstaff = (Map<String, Objects>) staffDao.selectRecycle(map.get("employeeId"));
                                    Map<String, Objects> staff = (Map<String, Objects>) staffDao.select(map.get("employeeId"));
                                    if (recstaff == null) {
                                        if (staff == null) {
                                            try {
                                                staffDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recstaff.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                staffDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{user}User:staffmobile":
                                    Map<String, Objects> recstaffmobile = (Map<String, Objects>) staffMobileDao.selectRecycle(map.get("mobileNumber"));
                                    Map<String, Objects> staffmobile = (Map<String, Objects>) staffMobileDao.select(map.get("mobileNumber"));
                                    if (recstaffmobile == null) {
                                        if (staffmobile == null) {
                                            try {
                                                staffMobileDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recstaffmobile.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                staffMobileDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{stock}Center":
                                    Map<String, Objects> recstockcenter = (Map<String, Objects>) stockCenterDao.selectRecycle(map.get("uid"));
                                    Map<String, Objects> stockcenter = (Map<String, Objects>) stockCenterDao.select(map.get("uid"));
                                    if (recstockcenter == null) {
                                        if (stockcenter == null) {
                                            try {
                                                stockCenterDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recstockcenter.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                stockCenterDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{stock}stock":
                                    Map<String, Objects> recstock = (Map<String, Objects>) stockDao.selectRecycle(map.get("stockId"));
                                    Map<String, Objects> stock = (Map<String, Objects>) stockDao.select(map.get("stockId"));
                                    if (recstock == null) {
                                        if (stock == null) {
                                            try {
                                                stockDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recstock.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                stockDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{stock}Store":
                                    Map<String, Objects> recstockstore = (Map<String, Objects>) stockStoreDao.selectRecycle(map.get("uid"));
                                    Map<String, Objects> stockstore = (Map<String, Objects>) stockStoreDao.select(map.get("uid"));
                                    if (recstockstore == null) {
                                        if (stockstore == null) {
                                            try {
                                                stockStoreDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recstockstore.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                stockStoreDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{base}Base:area":
                                    Map<String, Objects> rectaoarea = (Map<String, Objects>) taoAreaDao.selectRecycle(map.get("id"));
                                    Map<String, Objects> taoarea = (Map<String, Objects>) taoAreaDao.select(map.get("id"));
                                    if (rectaoarea == null) {
                                        if (taoarea == null) {
                                            try {
                                                taoAreaDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(rectaoarea.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                taoAreaDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{thirdParty}thirdParty:taobao":
                                    Map<String, Objects> rectaobao = (Map<String, Objects>) taobaoDao.selectRecycle(map.get("taobaoOpenId"));
                                    Map<String, Objects> taobao = (Map<String, Objects>) taobaoDao.select(map.get("taobaoOpenId"));
                                    if (rectaobao == null) {
                                        if (taobao == null) {
                                            try {
                                                taobaoDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(rectaobao.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                taobaoDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{test}Test":
                                    Map<String, Objects> rectest = (Map<String, Objects>) testDao.selectRecycle(map.get("testId"));
                                    Map<String, Objects> test = (Map<String, Objects>) testDao.select(map.get("testId"));
                                    if (rectest == null) {
                                        if (test == null) {
                                            try {
                                                testDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(rectest.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                testDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{thirdParty}thirdParty":
                                    Map<String, Objects> recthirdparty = (Map<String, Objects>) thirdPartyDao.selectRecycle(map.get("uid"));
                                    Map<String, Objects> thirdparty = (Map<String, Objects>) thirdPartyDao.select(map.get("uid"));
                                    if (recthirdparty == null) {
                                        if (thirdparty == null) {
                                            try {
                                                thirdPartyDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recthirdparty.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                thirdPartyDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{user}User:address":
                                    Map<String, Objects> recuseraddress = (Map<String, Objects>) userAddressDao.selectRecycle(map.get("addressId"));
                                    Map<String, Objects> useraddress = (Map<String, Objects>) userAddressDao.select(map.get("addressId"));
                                    if (recuseraddress == null) {
                                        if (useraddress == null) {
                                            try {
                                                userAddressDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recuseraddress.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                userAddressDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{user}User":
                                    Map<String, Objects> recuser = (Map<String, Objects>) userDao.selectRecycle(map.get("userId"));
                                    Map<String, Objects> user = (Map<String, Objects>) userDao.select(map.get("userId"));
                                    if (recuser == null) {
                                        if (user == null) {
                                            try {
                                                userDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recuser.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                userDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{user}User:favorite":
                                    Map<String, Objects> recuserfavorite = (Map<String, Objects>) userFavoriteDao.selectRecycle(map.get("uid"));
                                    Map<String, Objects> userfavorite = (Map<String, Objects>) userFavoriteDao.select(map.get("uid"));
                                    if (recuserfavorite == null) {
                                        if (userfavorite == null) {
                                            try {
                                                userFavoriteDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recuserfavorite.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                userFavoriteDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{user}User:info":
                                    Map<String, Objects> recuserinfo = (Map<String, Objects>) userInfoDao.selectRecycle(map.get("userId"));
                                    Map<String, Objects> userinfo = (Map<String, Objects>) userInfoDao.select(map.get("userId"));
                                    if (recuserinfo == null) {
                                        if (userinfo == null) {
                                            try {
                                                userInfoDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recuserinfo.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                userInfoDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{thirdParty}thirdParty:weibo":
                                    Map<String, Objects> recweibo = (Map<String, Objects>) weiboDao.selectRecycle(map.get("weiboOpenId"));
                                    Map<String, Objects> weibo = (Map<String, Objects>) weiboDao.select(map.get("weiboOpenId"));
                                    if (recweibo == null) {
                                        if (weibo == null) {
                                            try {
                                                weiboDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recweibo.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                weiboDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{thirdParty}thirdParty:weixin":
                                    Map<String, Objects> recweixi = (Map<String, Objects>) weixiDao.selectRecycle(map.get("weixinOpenId"));
                                    Map<String, Objects> weixi = (Map<String, Objects>) weixiDao.select(map.get("weixinOpenId"));
                                    if (recweixi == null) {
                                        if (weixi == null) {
                                            try {
                                                weixiDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recweixi.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                weixiDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{base}Base:zoneDeliveryMode":
                                    Map<String, Objects> reczonedeliverymode= (Map<String, Objects>) zoneDeliveryModeDao.selectRecycle(map.get("code"));
                                    Map<String, Objects> zonedeliverymode = (Map<String, Objects>) zoneDeliveryModeDao.select(map.get("code"));
                                    if (reczonedeliverymode == null) {
                                        if (zonedeliverymode == null) {
                                            try {
                                                zoneDeliveryModeDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(reczonedeliverymode.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                zoneDeliveryModeDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{base}Base:zoneDeliveryModeValue":
                                    Map<String, Objects> reczonedeliverymodevalue = (Map<String, Objects>) zoneDeliveryModeValueDao.selectRecycle(map.get("zoneDeliveryModeValueId"));
                                    Map<String, Objects> zonedeliverymodevalue = (Map<String, Objects>) zoneDeliveryModeValueDao.select(map.get("zoneDeliveryModeValueId"));
                                    if (reczonedeliverymodevalue == null) {
                                        if (zonedeliverymodevalue == null) {
                                            try {
                                                zoneDeliveryModeValueDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("过时数据，不进行操作。");
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(reczonedeliverymodevalue.get("version"))) > Integer.parseInt(map.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                zoneDeliveryModeValueDao.add(map);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;
                            }
                        } else if (names[0].equals("del")) {
                            HashMap<String, String> map2 = new HashMap<String, String>();
                            // 将json字符串转换成jsonObject
                            JSONObject jsonObject = JSONObject.fromObject(names[3]);
                            Iterator it = jsonObject.keys();
                            // 遍历jsonObject数据，添加到Map对象
                            while (it.hasNext()) {
                                String key1 = String.valueOf(it.next());
                                String value1 = jsonObject.get(key1).toString();
                                map2.put(key1, value1);
                            }
                            switch (names[1]) {
                                case "hmall:cache:{thirdParty}thirdParty:alipay":
                                    try {
                                        alipayDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> alipay = (Map<String, Objects>) alipayDao.select(names[2]);
                                    if (alipay == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(alipay.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                alipayDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

//                                case "hmall:cache:{base}Base:area":
//                                    break;

                                case "hmall:online:{product}Attr":
                                    try {
                                        attributionOnlineDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> attributiononline = (Map<String, Objects>) attributionOnlineDao.select(names[2]);
                                    if (attributiononline == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(attributiononline.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                attributionOnlineDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:online:{product}Attr:product":
                                    try {
                                        attributionRelationOnlineDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> attributionrelationonline = (Map<String, Objects>) attributionRelationOnlineDao.select(names[2]);
                                    if (attributionrelationonline == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(attributionrelationonline.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                attributionRelationOnlineDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:staged:{product}Attr:product":
                                    try {
                                        attributionRelationStagedDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> attributionrelationstaged = (Map<String, Objects>) attributionRelationStagedDao.select(names[2]);
                                    if (attributionrelationstaged == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(attributionrelationstaged.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                attributionRelationStagedDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:staged:{product}Attr":
                                    try {
                                        attributionStagedDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> attributionstaged = (Map<String, Objects>) attributionStagedDao.select(names[2]);
                                    if (attributionstaged == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(attributionstaged.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                attributionStagedDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{Base}Base:baseStore":
                                    try {
                                        baseStoreDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> basestore = (Map<String, Objects>) baseStoreDao.select(names[2]);
                                    if (basestore == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(basestore.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                baseStoreDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{verification}Captcha":
//                                    try {
//                                        staffDao.addRecycle(map2);
//                                    } catch (Exception e) {
//                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
//                                        System.out.println("数据库操作失败，转发kafka");
//                                    }
//                                    Map<String, Objects> staff = (Map<String, Objects>) staffDao.select(names[2]);
//                                    if (staff == null) {
//                                        System.out.println("操作过时");
//                                    } else {
//                                        if (Integer.parseInt(String.valueOf(staff.get("version"))) < Integer.parseInt(map2.get("version"))) {
//                                            try {
//                                                staffDao.delete(names[2]);
//                                            } catch (Exception e) {
//                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
//                                                System.out.println("数据库操作失败，转发kafka");
//                                            }
//                                        } else {
//                                            System.out.println("操作过时");
//                                        }
//                                    }
                                    break;

                                case "hmall:cache:{cart}Cart":
                                    try {
                                        cartDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> cart = (Map<String, Objects>) cartDao.select(names[2]);
                                    if (cart == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(cart.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                cartDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:online:{product}Category":
                                    try {
                                        categoryOnlineDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> categoryonline = (Map<String, Objects>) categoryOnlineDao.select(names[2]);
                                    if (categoryonline == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(categoryonline.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                categoryOnlineDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:online:{product}Category:product":
                                    try {
                                        categoryRelationOnlineDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> categoryrelationonline = (Map<String, Objects>) categoryRelationOnlineDao.select(names[2]);
                                    if (categoryrelationonline == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(categoryrelationonline.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                categoryRelationOnlineDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:staged:{product}Category:product":
                                    try {
                                        categoryRelationStagedDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> categoryrelationstaged = (Map<String, Objects>) categoryRelationStagedDao.select(names[2]);
                                    if (categoryrelationstaged == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(categoryrelationstaged.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                categoryRelationStagedDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:staged:{product}Category":
                                    try {
                                        categoryStagedDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> categorystaged = (Map<String, Objects>) categoryStagedDao.select(names[2]);
                                    if (categorystaged == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(categorystaged.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                categoryStagedDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{Base}Base:cMSSite":
                                    try {
                                        cmsSiteDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> cmssite = (Map<String, Objects>) cmsSiteDao.select(names[2]);
                                    if (cmssite == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(cmssite.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                cmsSiteDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{promotion}coupon":
                                    try {
                                        couponDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> coupon = (Map<String, Objects>) couponDao.select(names[2]);
                                    if (coupon == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(coupon.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                couponDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{promotion}coupon:customerCoupon":
                                    try {
                                        customerCouponDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> customercoupon = (Map<String, Objects>) customerCouponDao.select(names[2]);
                                    if (customercoupon == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(customercoupon.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                customerCouponDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{base}Base:deliveryTemplate":
                                    try {
                                        deliveryTemplateDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> deliverytemplate = (Map<String, Objects>) deliveryTemplateDao.select(names[2]);
                                    if (deliverytemplate == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(deliverytemplate.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                deliveryTemplateDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{user}User:emailpath":
                                    try {
                                        emailPathDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> emailpath = (Map<String, Objects>) emailPathDao.select(names[2]);
                                    if (emailpath == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(emailpath.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                emailPathDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{sms}Sms:error":
                                    try {
                                        errorDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> error = (Map<String, Objects>) errorDao.select(names[2]);
                                    if (error == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(error.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                errorDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{comment}Comment:evaluation":
                                    try {
                                        evaluationDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> evaluation = (Map<String, Objects>) evaluationDao.select(names[2]);
                                    if (evaluation == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(evaluation.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                evaluationDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{Base}Base:inStockStatus":
                                    try {
                                        inStockStatusDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> instockstatus = (Map<String, Objects>) inStockStatusDao.select(names[2]);
                                    if (instockstatus == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(instockstatus.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                inStockStatusDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{user}User:login":
                                    try {
                                        loginDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> login = (Map<String, Objects>) loginDao.select(names[2]);
                                    if (login == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(login.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                loginDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{base}Base:logisticsCompaniesDao":
                                    try {
                                        logisticsCompaniesDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> logisticscompanies = (Map<String, Objects>) logisticsCompaniesDao.select(names[2]);
                                    if (logisticscompanies == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(logisticscompanies.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                logisticsCompaniesDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{customer}Customer:mobile":
                                    try {
                                        mobileDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> mobile = (Map<String, Objects>) mobileDao.select(names[2]);
                                    if (mobile == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(mobile.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                mobileDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{order}Order":
                                    try {
                                        orderDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> order = (Map<String, Objects>) orderDao.select(names[2]);
                                    if (order == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(order.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                orderDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{order}Express":
                                    try {
                                        orderExpressDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> orderexpress = (Map<String, Objects>) orderExpressDao.select(names[2]);
                                    if (orderexpress == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(orderexpress.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                orderExpressDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{order}Order:node":
                                    try {
                                        orderNodeDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> ordernode = (Map<String, Objects>) orderNodeDao.select(names[2]);
                                    if (ordernode == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(ordernode.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                orderNodeDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{order}Pickup:code":
                                    try {
                                        orderPickupCodeDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> orderpickupcode = (Map<String, Objects>) orderPickupCodeDao.select(names[2]);
                                    if (orderpickupcode == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(orderpickupcode.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                orderPickupCodeDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{order}Pickup":
                                    try {
                                        orderPickupDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> orderpickup = (Map<String, Objects>) orderPickupDao.select(names[2]);
                                    if (orderpickup == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(orderpickup.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                orderPickupDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{order}Order:price":
                                    try {
                                        orderPriceDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> orderprice = (Map<String, Objects>) orderPriceDao.select(names[2]);
                                    if (orderprice == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(orderprice.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                orderPriceDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{order}Order:details":
                                    try {
                                        orderProductDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> orderproduct = (Map<String, Objects>) orderProductDao.select(names[2]);
                                    if (orderproduct == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(orderproduct.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                orderProductDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{order}Order:status":
                                    try {
                                        orderStatusDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> orderstatus = (Map<String, Objects>) orderStatusDao.select(names[2]);
                                    if (orderstatus == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(orderstatus.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                orderStatusDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{order}Order:temp":
                                    try {
                                        orderTempDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> ordertemp = (Map<String, Objects>) orderTempDao.select(names[2]);
                                    if (ordertemp == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(ordertemp.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                orderTempDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{base}Base:paymentMode":
                                    try {
                                        paymentModeDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> paymentmode = (Map<String, Objects>) paymentModeDao.select(names[2]);
                                    if (paymentmode == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(paymentmode.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                paymentModeDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{base}Base:pointOfService":
                                    try {
                                        pointOfServiceDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> pointofservice = (Map<String, Objects>) pointOfServiceDao.select(names[2]);
                                    if (pointofservice == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(pointofservice.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                pointOfServiceDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:online:{product}Product":
                                    try {
                                        productDetailOnlineDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> productdetailonline = (Map<String, Objects>) productDetailOnlineDao.select(names[2]);
                                    if (productdetailonline == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(productdetailonline.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                productDetailOnlineDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:staged:{product}Product":
                                    try {
                                        productDetailStagedDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> productdetailstaged = (Map<String, Objects>) productDetailStagedDao.select(names[2]);
                                    if (productdetailstaged == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(productdetailstaged.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                productDetailStagedDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:online:{product}Product:recommend":
                                    try {
                                        productRecommendOnlineDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> productrecommendonline = (Map<String, Objects>) productRecommendOnlineDao.select(names[2]);
                                    if (productrecommendonline == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(productrecommendonline.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                productRecommendOnlineDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:staged:{product}Product:recommend":
                                    try {
                                        productRecommendStagedDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> productrecommendstaged = (Map<String, Objects>) productRecommendStagedDao.select(names[2]);
                                    if (productrecommendstaged == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(productrecommendstaged.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                productRecommendStagedDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:online:{product}Product:resource":
                                    try {
                                        productResourceOnlineDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> productresourceonline = (Map<String, Objects>) productResourceOnlineDao.select(names[2]);
                                    if (productresourceonline == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(productresourceonline.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                productResourceOnlineDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:staged:{product}Product:resource":
                                    try {
                                        productResourceStagedDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> productresourcestaged = (Map<String, Objects>) productResourceStagedDao.select(names[2]);
                                    if (productresourcestaged == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(productresourcestaged.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                productResourceStagedDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:online:{product}Product:summary":
                                    try {
                                        productSummaryOnlineDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> productsummaryonline = (Map<String, Objects>) productSummaryOnlineDao.select(names[2]);
                                    if (productsummaryonline == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(productsummaryonline.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                productSummaryOnlineDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:staged:{product}Product:summary":
                                    try {
                                        productSummaryStagedDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> productsummarystaged = (Map<String, Objects>) productSummaryStagedDao.select(names[2]);
                                    if (productsummarystaged == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(productsummarystaged.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                productSummaryStagedDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{prompt}Prompt:zh":
                                    try {
                                        promptDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> prompt = (Map<String, Objects>) promptDao.select(names[2]);
                                    if (prompt == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(prompt.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                promptDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{comment}Comment:purchaseAdvice":
                                    try {
                                        purchaseAdviceDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> purchaseadvice = (Map<String, Objects>) purchaseAdviceDao.select(names[2]);
                                    if (purchaseadvice == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(purchaseadvice.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                purchaseAdviceDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{user}User:pwderror":
                                    try {
                                        pwdErrorDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> pwderror = (Map<String, Objects>) pwdErrorDao.select(names[2]);
                                    if (pwderror == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(pwderror.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                pwdErrorDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{thirdParty}thirdParty:qq":
                                    try {
                                        qqDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> qq = (Map<String, Objects>) qqDao.select(names[2]);
                                    if (qq == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(qq.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                qqDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{user}User:quicklogin":
                                    try {
                                        quickLoginDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> quicklogin = (Map<String, Objects>) quickLoginDao.select(names[2]);
                                    if (quicklogin == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(quicklogin.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                quickLoginDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{refund}Refund":
                                    try {
                                        refundDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> refund = (Map<String, Objects>) refundDao.select(names[2]);
                                    if (refund == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(refund.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                refundDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{rules}Rules:action":
                                    try {
                                        rulesActionDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> rulesaction = (Map<String, Objects>) rulesActionDao.select(names[2]);
                                    if (rulesaction == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(rulesaction.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                rulesActionDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{rules}Rules:condition":
                                    try {
                                        rulesConditionDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> rulescondition = (Map<String, Objects>) rulesConditionDao.select(names[2]);
                                    if (rulescondition == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(rulescondition.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                rulesConditionDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{rules}Rules:models":
                                    try {
                                        rulesModelDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> rulesmodel = (Map<String, Objects>) rulesModelDao.select(names[2]);
                                    if (rulesmodel == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(rulesmodel.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                rulesModelDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{rules}Rules:temp":
                                    try {
                                        rulesTmplDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> rulestmpl = (Map<String, Objects>) rulesTmplDao.select(names[2]);
                                    if (rulestmpl == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(rulestmpl.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                rulesTmplDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{SaleActivity}SaleActivity":
                                    try {
                                        saleActivityDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> saleactivity = (Map<String, Objects>) saleActivityDao.select(names[2]);
                                    if (saleactivity == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(saleactivity.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                saleActivityDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{SaleDiscountCoupon}SaleDiscountCoupon":
                                    try {
                                        saleDiscountCouponDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> salediscountcoupon = (Map<String, Objects>) saleDiscountCouponDao.select(names[2]);
                                    if (salediscountcoupon == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(salediscountcoupon.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                saleDiscountCouponDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{SaleTemplate}SaleTemplate":
                                    try {
                                        saleTemplateDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> saletemplate = (Map<String, Objects>) saleTemplateDao.select(names[2]);
                                    if (saletemplate == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(saletemplate.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                saleTemplateDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{replace}Replace:shippingReplacement":
                                    try {
                                        shippingReplacementDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> shippingreplacement = (Map<String, Objects>) shippingReplacementDao.select(names[2]);
                                    if (shippingreplacement == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(shippingreplacement.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                shippingReplacementDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{siteMessage}siteMessage":
                                    try {
                                        siteMsgDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> sitemsg = (Map<String, Objects>) siteMsgDao.select(names[2]);
                                    if (sitemsg == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(sitemsg.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                siteMsgDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{siteMessage}siteMessage:read":
                                    try {
                                        siteMsgReadDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> sitemsgread = (Map<String, Objects>) siteMsgReadDao.select(names[2]);
                                    if (sitemsgread == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(sitemsgread.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                siteMsgReadDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{siteMessage}siteMessage:type":
                                    try {
                                        siteMsgTypeDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> sitemsgtype = (Map<String, Objects>) siteMsgTypeDao.select(names[2]);
                                    if (sitemsgtype == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(sitemsgtype.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                siteMsgTypeDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{verification}SliderCaptcha":
//                                    try {
//                                        staffDao.addRecycle(map2);
//                                    } catch (Exception e) {
//                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
//                                        System.out.println("数据库操作失败，转发kafka");
//                                    }
//                                    Map<String, Objects> staff = (Map<String, Objects>) staffDao.select(names[2]);
//                                    if (staff == null) {
//                                        System.out.println("操作过时");
//                                    } else {
//                                        if (Integer.parseInt(String.valueOf(staff.get("version"))) < Integer.parseInt(map2.get("version"))) {
//                                            try {
//                                                staffDao.delete(names[2]);
//                                            } catch (Exception e) {
//                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
//                                                System.out.println("数据库操作失败，转发kafka");
//                                            }
//                                        } else {
//                                            System.out.println("操作过时");
//                                        }
//                                    }
                                    break;

                                case "hmall:cache:{sms}Sms":
                                    try {
                                        smsDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> sms = (Map<String, Objects>) smsDao.select(names[2]);
                                    if (sms == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(sms.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                smsDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{base}Base:staff":
                                    try {
                                        staffDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> staff = (Map<String, Objects>) staffDao.select(names[2]);
                                    if (staff == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(staff.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                staffDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{user}User:staffmobile":
                                    try {
                                        staffMobileDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> staffmobile = (Map<String, Objects>) staffMobileDao.select(names[2]);
                                    if (staffmobile == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(staffmobile.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                staffMobileDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{stock}Center":
                                    try {
                                        stockCenterDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> stockcenter = (Map<String, Objects>) stockCenterDao.select(names[2]);
                                    if (stockcenter == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(stockcenter.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                stockCenterDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{stock}stock":
                                    try {
                                        stockDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> stock = (Map<String, Objects>) stockDao.select(names[2]);
                                    if (stock == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(stock.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                stockDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{stock}Store":
                                    try {
                                        stockStoreDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> stockstore = (Map<String, Objects>) stockStoreDao.select(names[2]);
                                    if (stockstore == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(stockstore.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                stockStoreDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{base}Base:area":
                                    try {
                                        taoAreaDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> taoarea = (Map<String, Objects>) taoAreaDao.select(names[2]);
                                    if (taoarea == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(taoarea.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                taoAreaDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{thirdParty}thirdParty:taobao":
                                    try {
                                        taobaoDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> taobao = (Map<String, Objects>) taobaoDao.select(names[2]);
                                    if (taobao == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(taobao.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                taobaoDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{test}Test":
                                    try {
                                        testDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> test = (Map<String, Objects>) testDao.select(names[2]);
                                    if (test == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(test.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                testDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{thirdParty}thirdParty":
                                    try {
                                        thirdPartyDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> thirdparty = (Map<String, Objects>) thirdPartyDao.select(names[2]);
                                    if (thirdparty == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(thirdparty.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                thirdPartyDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{user}User:address":
                                    try {
                                        userAddressDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> useraddress = (Map<String, Objects>) userAddressDao.select(names[2]);
                                    if (useraddress == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(useraddress.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                userAddressDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{user}User":
                                    try {
                                        userDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> user = (Map<String, Objects>) userDao.select(names[2]);
                                    if (user == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(user.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                userDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{user}User:favorite":
                                    try {
                                        userFavoriteDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> userfavorite = (Map<String, Objects>) userFavoriteDao.select(names[2]);
                                    if (userfavorite == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(userfavorite.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                userFavoriteDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{user}User:info":
                                    try {
                                        userInfoDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> userinfo = (Map<String, Objects>) userInfoDao.select(names[2]);
                                    if (userinfo == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(userinfo.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                userInfoDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{thirdParty}thirdParty:weibo":
                                    try {
                                        weiboDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> weibo = (Map<String, Objects>) weiboDao.select(names[2]);
                                    if (weibo == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(weibo.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                weiboDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{thirdParty}thirdParty:weixin":
                                    try {
                                        weixiDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> weixi = (Map<String, Objects>) weixiDao.select(names[2]);
                                    if (weixi == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(weixi.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                weixiDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{base}Base:zoneDeliveryMode":
                                    try {
                                        zoneDeliveryModeDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> zonedeliverymode = (Map<String, Objects>) zoneDeliveryModeDao.select(names[2]);
                                    if (zonedeliverymode == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(zonedeliverymode.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                zoneDeliveryModeDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;

                                case "hmall:cache:{base}Base:zoneDeliveryModeValue":
                                    try {
                                        zoneDeliveryModeValueDao.addRecycle(map2);
                                    } catch (Exception e) {
                                        kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                        System.out.println("数据库操作失败，转发kafka");
                                    }
                                    Map<String, Objects> zonedeliverymodevalue = (Map<String, Objects>) zoneDeliveryModeValueDao.select(names[2]);
                                    if (zonedeliverymodevalue == null) {
                                        System.out.println("操作过时");
                                    } else {
                                        if (Integer.parseInt(String.valueOf(zonedeliverymodevalue.get("version"))) < Integer.parseInt(map2.get("version"))) {
                                            try {
                                                zoneDeliveryModeValueDao.delete(names[2]);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map2));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        } else {
                                            System.out.println("操作过时");
                                        }
                                    }
                                    break;
                            }
                        } else if (names[0].equals("up")) {
                            HashMap<String, String> map1 = new HashMap<String, String>();
                            // 将json字符串转换成jsonObject
                            JSONObject jsonObject = JSONObject.fromObject(names[2]);
                            Iterator it = jsonObject.keys();
                            // 遍历jsonObject数据，添加到Map对象
                            while (it.hasNext()) {
                                String key1 = String.valueOf(it.next());
                                String value1 = jsonObject.get(key1).toString();
                                map1.put(key1, value1);
                            }
                            switch (names[1]) {
                                case "hmall:cache:{thirdParty}thirdParty:alipay":
                                    Map<String, Objects> recalipay = (Map<String, Objects>) alipayDao.selectRecycle(map1.get("alipayOpenId"));
                                    Map<String, Objects> alipay = (Map<String, Objects>) alipayDao.select(map1.get("alipayOpenId"));
                                    if (recalipay == null) {
                                        if (alipay == null) {
                                            try {
                                                alipayDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(alipay.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    alipayDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recalipay.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                alipayDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

//                                case "hmall:cache:{base}Base:area":
//                                    break;

                                case "hmall:online:{product}Attr":
                                    Map<String, Objects> recattributiononline = (Map<String, Objects>) attributionOnlineDao.selectRecycle(map1.get("attrId"));
                                    Map<String, Objects> attributiononline = (Map<String, Objects>) attributionOnlineDao.select(map1.get("attrId"));
                                    if (recattributiononline == null) {
                                        if (attributiononline == null) {
                                            try {
                                                attributionOnlineDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(attributiononline.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    attributionOnlineDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recattributiononline.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                attributionOnlineDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:online:{product}Attr:product":
                                    Map<String, Objects> recattributionrelationonline = (Map<String, Objects>) attributionRelationOnlineDao.selectRecycle(map1.get("uid"));
                                    Map<String, Objects> attributionrelationonline = (Map<String, Objects>) attributionRelationOnlineDao.select(map1.get("uid"));
                                    if (recattributionrelationonline == null) {
                                        if (attributionrelationonline == null) {
                                            try {
                                                attributionRelationOnlineDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(attributionrelationonline.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    attributionRelationOnlineDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recattributionrelationonline.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                attributionRelationOnlineDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:staged:{product}Attr:product":
                                    Map<String, Objects> recattributionrelationstaged = (Map<String, Objects>) attributionRelationStagedDao.selectRecycle(map1.get("uid"));
                                    Map<String, Objects> attributionrelationstaged = (Map<String, Objects>) attributionRelationStagedDao.select(map1.get("uid"));
                                    if (recattributionrelationstaged == null) {
                                        if (attributionrelationstaged == null) {
                                            try {
                                                attributionRelationStagedDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(attributionrelationstaged.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    attributionRelationStagedDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recattributionrelationstaged.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                attributionRelationStagedDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:staged:{product}Attr":
                                    Map<String, Objects> recattributionstaged = (Map<String, Objects>) attributionStagedDao.selectRecycle(map1.get("attrId"));
                                    Map<String, Objects> attributionstaged = (Map<String, Objects>) attributionStagedDao.select(map1.get("attrId"));
                                    if (recattributionstaged == null) {
                                        if (attributionstaged == null) {
                                            try {
                                                attributionStagedDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(attributionstaged.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    attributionStagedDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recattributionstaged.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                attributionStagedDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{Base}Base:baseStore":
                                    Map<String, Objects> recbasestore = (Map<String, Objects>) baseStoreDao.selectRecycle(map1.get("storeId"));
                                    Map<String, Objects> basestore = (Map<String, Objects>) baseStoreDao.select(map1.get("storeId"));
                                    if (recbasestore == null) {
                                        if (basestore == null) {
                                            try {
                                                baseStoreDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(basestore.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    baseStoreDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recbasestore.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                baseStoreDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{verification}Captcha":
//                                    Map<String, Objects> recstaff = (Map<String, Objects>) staffDao.selectRecycle(map1.get("employeeId"));
//                                    Map<String, Objects> staff = (Map<String, Objects>) staffDao.select(map1.get("employeeId"));
//                                    if (recstaff == null) {
//                                        if (staff == null) {
//                                            try {
//                                                staffDao.add(map1);
//                                            } catch (Exception e) {
//                                                kafkaproduce.kp(String.valueOf(map1));
//                                                System.out.println("操作失败，转发kafka");
//                                            }
//                                        } else {
//                                            if (Integer.parseInt(String.valueOf(staff.get("version"))) < Integer.parseInt(map1.get("version"))) {
//                                                try {
//                                                    staffDao.update(map1);
//                                                } catch (Exception e) {
//                                                    kafkaproduce.kp(String.valueOf(map1));
//                                                    System.out.println("操作失败，转发kafka");
//                                                }
//                                            } else {
//                                                System.out.println("过时数据，不进行操作。");
//                                            }
//                                        }
//                                    } else {
//                                        if (Integer.parseInt(String.valueOf(recstaff.get("version"))) > Integer.parseInt(map1.get("version"))) {
//                                            System.out.println("过时数据，不进行操作。");
//                                        } else {
//                                            try {
//                                                staffDao.add(map1);
//                                            } catch (Exception e) {
//                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
//                                                System.out.println("数据库操作失败，转发kafka");
//                                            }
//                                        }
//                                    }
                                    break;

                                case "hmall:cache:{cart}Cart":
                                    Map<String, Objects> reccart = (Map<String, Objects>) cartDao.selectRecycle(map1.get("cartId"));
                                    Map<String, Objects> cart = (Map<String, Objects>) cartDao.select(map1.get("cartId"));
                                    if (reccart == null) {
                                        if (cart == null) {
                                            try {
                                                cartDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(cart.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    cartDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(reccart.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                cartDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:online:{product}Category":
                                    Map<String, Objects> reccategoryonline = (Map<String, Objects>) categoryOnlineDao.selectRecycle(map1.get("uid"));
                                    Map<String, Objects> categoryonline = (Map<String, Objects>) categoryOnlineDao.select(map1.get("uid"));
                                    if (reccategoryonline == null) {
                                        if (categoryonline == null) {
                                            try {
                                                categoryOnlineDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(categoryonline.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    categoryOnlineDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(reccategoryonline.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                categoryOnlineDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:online:{product}Category:product":
                                    Map<String, Objects> reccategoryrelationonline = (Map<String, Objects>) categoryRelationOnlineDao.selectRecycle(map1.get("uid"));
                                    Map<String, Objects> categoryrelationonline = (Map<String, Objects>) categoryRelationOnlineDao.select(map1.get("uid"));
                                    if (reccategoryrelationonline == null) {
                                        if (categoryrelationonline == null) {
                                            try {
                                                categoryRelationOnlineDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(categoryrelationonline.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    categoryRelationOnlineDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(reccategoryrelationonline.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                categoryRelationOnlineDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:staged:{product}Category:product":
                                    Map<String, Objects> reccategoryrelationstaged = (Map<String, Objects>) categoryRelationStagedDao.selectRecycle(map1.get("uid"));
                                    Map<String, Objects> categoryrelationstaged = (Map<String, Objects>) categoryRelationStagedDao.select(map1.get("uid"));
                                    if (reccategoryrelationstaged == null) {
                                        if (categoryrelationstaged == null) {
                                            try {
                                                categoryRelationStagedDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(categoryrelationstaged.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    categoryRelationStagedDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(reccategoryrelationstaged.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                categoryRelationStagedDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:staged:{product}Category":
                                    Map<String, Objects> reccategorystaged = (Map<String, Objects>) categoryStagedDao.selectRecycle(map1.get("uid"));
                                    Map<String, Objects> categorystaged = (Map<String, Objects>) categoryStagedDao.select(map1.get("uid"));
                                    if (reccategorystaged == null) {
                                        if (categorystaged == null) {
                                            try {
                                                categoryStagedDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(categorystaged.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    categoryStagedDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(reccategorystaged.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                categoryStagedDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{Base}Base:cMSSite":
                                    Map<String, Objects> reccmssite = (Map<String, Objects>) cmsSiteDao.selectRecycle(map1.get("cmmsId"));
                                    Map<String, Objects> cmssite = (Map<String, Objects>) cmsSiteDao.select(map1.get("cmmsId"));
                                    if (reccmssite == null) {
                                        if (cmssite == null) {
                                            try {
                                                cmsSiteDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(cmssite.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    cmsSiteDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(reccmssite.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                cmsSiteDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{promotion}coupon":
                                    Map<String, Objects> reccoupon = (Map<String, Objects>) couponDao.selectRecycle(map1.get("couponCode"));
                                    Map<String, Objects> coupon = (Map<String, Objects>) couponDao.select(map1.get("couponCode"));
                                    if (reccoupon == null) {
                                        if (coupon == null) {
                                            try {
                                                couponDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(coupon.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    couponDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(reccoupon.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                couponDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{promotion}coupon:customerCoupon":
                                    Map<String, Objects> reccustomercoupon = (Map<String, Objects>) customerCouponDao.selectRecycle(map1.get("couponId"));
                                    Map<String, Objects> customercoupon = (Map<String, Objects>) customerCouponDao.select(map1.get("couponId"));
                                    if (reccustomercoupon == null) {
                                        if (customercoupon == null) {
                                            try {
                                                customerCouponDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(customercoupon.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    customerCouponDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(reccustomercoupon.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                customerCouponDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{base}Base:deliveryTemplate":
                                    Map<String, Objects> recdeliverytemplate = (Map<String, Objects>) deliveryTemplateDao.selectRecycle(map1.get("deliveryTemplateId"));
                                    Map<String, Objects> deliverytemplate = (Map<String, Objects>) deliveryTemplateDao.select(map1.get("deliveryTemplateId"));
                                    if (recdeliverytemplate == null) {
                                        if (deliverytemplate == null) {
                                            try {
                                                deliveryTemplateDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(deliverytemplate.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    deliveryTemplateDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recdeliverytemplate.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                deliveryTemplateDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{user}User:emailpath":
                                    Map<String, Objects> recemailpath = (Map<String, Objects>) emailPathDao.selectRecycle(map1.get("uid"));
                                    Map<String, Objects> emailpath = (Map<String, Objects>) emailPathDao.select(map1.get("uid"));
                                    if (recemailpath == null) {
                                        if (emailpath == null) {
                                            try {
                                                emailPathDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(emailpath.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    emailPathDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recemailpath.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                emailPathDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{sms}Sms:error":
                                    Map<String, Objects> recerror = (Map<String, Objects>) errorDao.selectRecycle(map1.get("uid"));
                                    Map<String, Objects> error = (Map<String, Objects>) errorDao.select(map1.get("uid"));
                                    if (recerror == null) {
                                        if (error == null) {
                                            try {
                                                errorDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(error.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    errorDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recerror.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                errorDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{comment}Comment:evaluation":
                                    Map<String, Objects> recevaluation = (Map<String, Objects>) evaluationDao.selectRecycle(map1.get("uid"));
                                    Map<String, Objects> evaluation = (Map<String, Objects>) evaluationDao.select(map1.get("uid"));
                                    if (recevaluation == null) {
                                        if (evaluation == null) {
                                            try {
                                                evaluationDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(evaluation.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    evaluationDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recevaluation.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                evaluationDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{Base}Base:inStockStatus":
                                    Map<String, Objects> recinstockstatus = (Map<String, Objects>) inStockStatusDao.selectRecycle(map1.get("inStockCode"));
                                    Map<String, Objects> instockstatus = (Map<String, Objects>) inStockStatusDao.select(map1.get("inStockCode"));
                                    if (recinstockstatus == null) {
                                        if (instockstatus == null) {
                                            try {
                                                inStockStatusDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(instockstatus.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    inStockStatusDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recinstockstatus.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                inStockStatusDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{user}User:login":
                                    Map<String, Objects> reclogin = (Map<String, Objects>) loginDao.selectRecycle(map1.get("customerId"));
                                    Map<String, Objects> login = (Map<String, Objects>) loginDao.select(map1.get("customerId"));
                                    if (reclogin== null) {
                                        if (login == null) {
                                            try {
                                                loginDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(login.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    loginDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(reclogin.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                loginDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{base}Base:logisticsCompaniesDao":
                                    Map<String, Objects> reclogisticscompanies = (Map<String, Objects>) logisticsCompaniesDao.selectRecycle(map1.get("code"));
                                    Map<String, Objects> logisticscompanies = (Map<String, Objects>) logisticsCompaniesDao.select(map1.get("code"));
                                    if (reclogisticscompanies == null) {
                                        if (logisticscompanies == null) {
                                            try {
                                                logisticsCompaniesDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(logisticscompanies.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    logisticsCompaniesDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(reclogisticscompanies.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                logisticsCompaniesDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{customer}Customer:mobile":
                                    Map<String, Objects> recmobile = (Map<String, Objects>) mobileDao.selectRecycle(map1.get("mobileNumber"));
                                    Map<String, Objects> mobile = (Map<String, Objects>) mobileDao.select(map1.get("mobileNumber"));
                                    if (recmobile == null) {
                                        if (mobile == null) {
                                            try {
                                                mobileDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(mobile.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    mobileDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recmobile.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                mobileDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{order}Order":
                                    Map<String, Objects> recorder = (Map<String, Objects>) orderDao.selectRecycle(map1.get("orderId"));
                                    Map<String, Objects> order = (Map<String, Objects>) orderDao.select(map1.get("orderId"));
                                    if (recorder == null) {
                                        if (order == null) {
                                            try {
                                                orderDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(order.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    orderDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recorder.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                orderDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{order}Express":
                                    Map<String, Objects> recorderexpress = (Map<String, Objects>) orderExpressDao.selectRecycle(map1.get("expressId"));
                                    Map<String, Objects> orderexpress = (Map<String, Objects>) orderExpressDao.select(map1.get("expressId"));
                                    if (recorderexpress == null) {
                                        if (orderexpress == null) {
                                            try {
                                                orderExpressDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(orderexpress.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    orderExpressDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recorderexpress.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                orderExpressDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{order}Order:node":
                                    Map<String, Objects> recordernode = (Map<String, Objects>) orderNodeDao.selectRecycle(map1.get("uid"));
                                    Map<String, Objects> ordernode = (Map<String, Objects>) orderNodeDao.select(map1.get("uid"));
                                    if (recordernode == null) {
                                        if (ordernode == null) {
                                            try {
                                                orderNodeDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(ordernode.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    orderNodeDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recordernode.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                orderNodeDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{order}Pickup:code":
                                    Map<String, Objects> recorderpickupcode = (Map<String, Objects>) orderPickupCodeDao.selectRecycle(map1.get("uid"));
                                    Map<String, Objects> orderpickupcode = (Map<String, Objects>) orderPickupCodeDao.select(map1.get("uid"));
                                    if (recorderpickupcode == null) {
                                        if (orderpickupcode == null) {
                                            try {
                                                orderPickupCodeDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(orderpickupcode.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    orderPickupCodeDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recorderpickupcode.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                orderPickupCodeDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{order}Pickup":
                                    Map<String, Objects> recorderpickup = (Map<String, Objects>) orderPickupDao.selectRecycle(map1.get("pickupId"));
                                    Map<String, Objects> orderpickup = (Map<String, Objects>) orderPickupDao.select(map1.get("pickupId"));
                                    if (recorderpickup == null) {
                                        if (orderpickup == null) {
                                            try {
                                                orderPickupDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(orderpickup.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    orderPickupDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recorderpickup.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                orderPickupDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{order}Order:price":
                                    Map<String, Objects> recorderprice = (Map<String, Objects>) orderPriceDao.selectRecycle(map1.get("orderId"));
                                    Map<String, Objects> orderprice = (Map<String, Objects>) orderPriceDao.select(map1.get("orderId"));
                                    if (recorderprice == null) {
                                        if (orderprice == null) {
                                            try {
                                                orderPriceDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(orderprice.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    orderPriceDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recorderprice.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                orderPriceDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{order}Order:details":
                                    Map<String, Objects> recorderproduct = (Map<String, Objects>) orderProductDao.selectRecycle(map1.get("uid"));
                                    Map<String, Objects> orderproduct = (Map<String, Objects>) orderProductDao.select(map1.get("uid"));
                                    if (recorderproduct == null) {
                                        if (orderproduct == null) {
                                            try {
                                                orderProductDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(orderproduct.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    orderProductDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recorderproduct.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                orderProductDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{order}Order:status":
                                    Map<String, Objects> recorderstatus = (Map<String, Objects>) orderStatusDao.selectRecycle(map1.get("orderId"));
                                    Map<String, Objects> orderstatus = (Map<String, Objects>) orderStatusDao.select(map1.get("orderId"));
                                    if (recorderstatus == null) {
                                        if (orderstatus == null) {
                                            try {
                                                orderStatusDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(orderstatus.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    orderStatusDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recorderstatus.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                orderStatusDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{order}Order:temp":
                                    Map<String, Objects> recordertemp = (Map<String, Objects>) orderTempDao.selectRecycle(map1.get("tempId"));
                                    Map<String, Objects> ordertemp = (Map<String, Objects>) orderTempDao.select(map1.get("tempId"));
                                    if (recordertemp == null) {
                                        if (ordertemp == null) {
                                            try {
                                                orderTempDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(ordertemp.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    orderTempDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recordertemp.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                orderTempDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{base}Base:paymentMode":
                                    Map<String, Objects> recpaymentmode = (Map<String, Objects>) paymentModeDao.selectRecycle(map1.get("paymentModeId"));
                                    Map<String, Objects> paymentmode = (Map<String, Objects>) paymentModeDao.select(map1.get("paymentModeId"));
                                    if (recpaymentmode == null) {
                                        if (paymentmode == null) {
                                            try {
                                                paymentModeDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(paymentmode.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    paymentModeDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recpaymentmode.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                paymentModeDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{base}Base:pointOfService":
                                    Map<String, Objects> recpointofservice = (Map<String, Objects>) pointOfServiceDao.selectRecycle(map1.get("code"));
                                    Map<String, Objects> pointofservice = (Map<String, Objects>) pointOfServiceDao.select(map1.get("code"));
                                    if (recpointofservice == null) {
                                        if (pointofservice == null) {
                                            try {
                                                pointOfServiceDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(pointofservice.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    pointOfServiceDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recpointofservice.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                pointOfServiceDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:online:{product}Product":
                                    Map<String, Objects> recproductdetailonline = (Map<String, Objects>) productDetailOnlineDao.selectRecycle(map1.get("productId"));
                                    Map<String, Objects> productdetailonline = (Map<String, Objects>) productDetailOnlineDao.select(map1.get("productId"));
                                    if (recproductdetailonline == null) {
                                        if (productdetailonline == null) {
                                            try {
                                                productDetailOnlineDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(productdetailonline.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    productDetailOnlineDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recproductdetailonline.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                productDetailOnlineDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:staged:{product}Product":
                                    Map<String, Objects> recroductdetailstaged = (Map<String, Objects>) productDetailStagedDao.selectRecycle(map1.get("productId"));
                                    Map<String, Objects> roductdetailstaged = (Map<String, Objects>) productDetailStagedDao.select(map1.get("productId"));
                                    if (recroductdetailstaged == null) {
                                        if (roductdetailstaged == null) {
                                            try {
                                                productDetailStagedDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(roductdetailstaged.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    productDetailStagedDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recroductdetailstaged.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                productDetailStagedDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:online:{product}Product:recommend":
                                    Map<String, Objects> recproductrecommendonline = (Map<String, Objects>) productRecommendOnlineDao.selectRecycle(map1.get("uid"));
                                    Map<String, Objects> productrecommendonline = (Map<String, Objects>) productRecommendOnlineDao.select(map1.get("uid"));
                                    if (recproductrecommendonline == null) {
                                        if (productrecommendonline == null) {
                                            try {
                                                productRecommendOnlineDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(productrecommendonline.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    productRecommendOnlineDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recproductrecommendonline.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                productRecommendOnlineDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:staged:{product}Product:recommend":
                                    Map<String, Objects> recproductrecommendstaged = (Map<String, Objects>) productRecommendStagedDao.selectRecycle(map1.get("uid"));
                                    Map<String, Objects> productrecommendstaged = (Map<String, Objects>) productRecommendStagedDao.select(map1.get("uid"));
                                    if (recproductrecommendstaged == null) {
                                        if (productrecommendstaged == null) {
                                            try {
                                                productRecommendStagedDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(productrecommendstaged.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    productRecommendStagedDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recproductrecommendstaged.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                productRecommendStagedDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:online:{product}Product:resource":
                                    Map<String, Objects> recproductresourceonline = (Map<String, Objects>) productResourceOnlineDao.selectRecycle(map1.get("productCode"));
                                    Map<String, Objects> productresourceonline = (Map<String, Objects>) productResourceOnlineDao.select(map1.get("productCode"));
                                    if (recproductresourceonline == null) {
                                        if (productresourceonline == null) {
                                            try {
                                                productResourceOnlineDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(productresourceonline.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    productResourceOnlineDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recproductresourceonline.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                productResourceOnlineDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:staged:{product}Product:resource":
                                    Map<String, Objects> recproductresourcestaged = (Map<String, Objects>) productResourceStagedDao.selectRecycle(map1.get("productCode"));
                                    Map<String, Objects> productresourcestaged = (Map<String, Objects>) productResourceStagedDao.select(map1.get("productCode"));
                                    if (recproductresourcestaged == null) {
                                        if (productresourcestaged == null) {
                                            try {
                                                productResourceStagedDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(productresourcestaged.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    productResourceStagedDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recproductresourcestaged.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                productResourceStagedDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:online:{product}Product:summary":
                                    Map<String, Objects> recproductsummaryonline = (Map<String, Objects>) productSummaryOnlineDao.selectRecycle(map1.get("productCode"));
                                    Map<String, Objects> productsummaryonline = (Map<String, Objects>) productSummaryOnlineDao.select(map1.get("productCode"));
                                    if (recproductsummaryonline == null) {
                                        if (productsummaryonline == null) {
                                            try {
                                                productSummaryOnlineDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(productsummaryonline.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    productSummaryOnlineDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recproductsummaryonline.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                productSummaryOnlineDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:staged:{product}Product:summary":
                                    Map<String, Objects> recproductsummarystaged = (Map<String, Objects>) productSummaryStagedDao.selectRecycle(map1.get("productCode"));
                                    Map<String, Objects> productsummarystaged = (Map<String, Objects>) productSummaryStagedDao.select(map1.get("productCode"));
                                    if (recproductsummarystaged == null) {
                                        if (productsummarystaged == null) {
                                            try {
                                                productSummaryStagedDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(productsummarystaged.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    productSummaryStagedDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recproductsummarystaged.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                productSummaryStagedDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{prompt}Prompt:zh":
                                    Map<String, Objects> recprompt = (Map<String, Objects>) promptDao.selectRecycle(map1.get("msgCode"));
                                    Map<String, Objects> prompt = (Map<String, Objects>) promptDao.select(map1.get("msgCode"));
                                    if (recprompt == null) {
                                        if (prompt == null) {
                                            try {
                                                promptDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(prompt.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    promptDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recprompt.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                promptDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{comment}Comment:purchaseAdvice":
                                    Map<String, Objects> recpurchaseadvice = (Map<String, Objects>) purchaseAdviceDao.selectRecycle(map1.get("uid"));
                                    Map<String, Objects> purchaseadvice = (Map<String, Objects>) purchaseAdviceDao.select(map1.get("uid"));
                                    if (recpurchaseadvice == null) {
                                        if (purchaseadvice == null) {
                                            try {
                                                purchaseAdviceDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(purchaseadvice.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    purchaseAdviceDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recpurchaseadvice.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                purchaseAdviceDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{user}User:pwderror":
                                    Map<String, Objects> recpwderror = (Map<String, Objects>) pwdErrorDao.selectRecycle(map1.get("uid"));
                                    Map<String, Objects> pwderror = (Map<String, Objects>) pwdErrorDao.select(map1.get("uid"));
                                    if (recpwderror == null) {
                                        if (pwderror == null) {
                                            try {
                                                pwdErrorDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(pwderror.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    pwdErrorDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recpwderror.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                pwdErrorDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{thirdParty}thirdParty:qq":
                                    Map<String, Objects> recqq = (Map<String, Objects>) qqDao.selectRecycle(map1.get("qqOpenId"));
                                    Map<String, Objects> qq = (Map<String, Objects>) qqDao.select(map1.get("qqOpenId"));
                                    if (recqq == null) {
                                        if (qq == null) {
                                            try {
                                                qqDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(qq.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    qqDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recqq.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                qqDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{user}User:quicklogin":
                                    Map<String, Objects> recquicklogin = (Map<String, Objects>) quickLoginDao.selectRecycle(map1.get("uid"));
                                    Map<String, Objects> quicklogin = (Map<String, Objects>) quickLoginDao.select(map1.get("uid"));
                                    if (recquicklogin == null) {
                                        if (quicklogin == null) {
                                            try {
                                                quickLoginDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(quicklogin.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    quickLoginDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recquicklogin.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                quickLoginDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{refund}Refund":
                                    Map<String, Objects> recrefund = (Map<String, Objects>) refundDao.selectRecycle(map1.get("refundId"));
                                    Map<String, Objects> refund = (Map<String, Objects>) refundDao.select(map1.get("refundId"));
                                    if (recrefund == null) {
                                        if (refund == null) {
                                            try {
                                                refundDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(refund.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    refundDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recrefund.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                refundDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{rules}Rules:action":
                                    Map<String, Objects> recrulesaction = (Map<String, Objects>) rulesActionDao.selectRecycle(map1.get("actionId"));
                                    Map<String, Objects> rulesaction = (Map<String, Objects>) rulesActionDao.select(map1.get("actionId"));
                                    if (recrulesaction == null) {
                                        if (rulesaction == null) {
                                            try {
                                                rulesActionDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(rulesaction.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    rulesActionDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recrulesaction.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                rulesActionDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{rules}Rules:condition":
                                    Map<String, Objects> recrulescondition = (Map<String, Objects>) rulesConditionDao.selectRecycle(map1.get("conditionId"));
                                    Map<String, Objects> rulescondition = (Map<String, Objects>) rulesConditionDao.select(map1.get("conditionId"));
                                    if (recrulescondition == null) {
                                        if (rulescondition == null) {
                                            try {
                                                rulesConditionDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(rulescondition.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    rulesConditionDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recrulescondition.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                rulesConditionDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{rules}Rules:models":
                                    Map<String, Objects> recrulesmode = (Map<String, Objects>) rulesModelDao.selectRecycle(map1.get("modelId"));
                                    Map<String, Objects> rulesmode = (Map<String, Objects>) rulesModelDao.select(map1.get("modelId"));
                                    if (recrulesmode == null) {
                                        if (rulesmode == null) {
                                            try {
                                                rulesModelDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(rulesmode.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    rulesModelDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recrulesmode.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                rulesModelDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{rules}Rules:temp":
                                    Map<String, Objects> recrulestmpl = (Map<String, Objects>) rulesTmplDao.selectRecycle(map1.get("tmplId"));
                                    Map<String, Objects> rulestmpl = (Map<String, Objects>) rulesTmplDao.select(map1.get("tmplId"));
                                    if (recrulestmpl == null) {
                                        if (rulestmpl == null) {
                                            try {
                                                rulesTmplDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(rulestmpl.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    rulesTmplDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recrulestmpl.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                rulesTmplDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{SaleActivity}SaleActivity":
                                    Map<String, Objects> recsaleactivity = (Map<String, Objects>) saleActivityDao.selectRecycle(map1.get("id"));
                                    Map<String, Objects> saleactivity = (Map<String, Objects>) saleActivityDao.select(map1.get("id"));
                                    if (recsaleactivity == null) {
                                        if (saleactivity == null) {
                                            try {
                                                saleActivityDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(saleactivity.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    saleActivityDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recsaleactivity.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                saleActivityDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{SaleDiscountCoupon}SaleDiscountCoupon":
                                    Map<String, Objects> recsalediscountcoupon = (Map<String, Objects>) saleDiscountCouponDao.selectRecycle(map1.get("id"));
                                    Map<String, Objects> salediscountcoupon = (Map<String, Objects>) saleDiscountCouponDao.select(map1.get("id"));
                                    if (recsalediscountcoupon == null) {
                                        if (salediscountcoupon == null) {
                                            try {
                                                saleDiscountCouponDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(salediscountcoupon.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    saleDiscountCouponDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recsalediscountcoupon.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                saleDiscountCouponDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{SaleTemplate}SaleTemplate":
                                    Map<String, Objects> recsaletemplate = (Map<String, Objects>) saleTemplateDao.selectRecycle(map1.get("id"));
                                    Map<String, Objects> saletemplate = (Map<String, Objects>) saleTemplateDao.select(map1.get("id"));
                                    if (recsaletemplate == null) {
                                        if (saletemplate == null) {
                                            try {
                                                saleTemplateDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(saletemplate.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    saleTemplateDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recsaletemplate.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                saleTemplateDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{replace}Replace:shippingReplacement":
                                    Map<String, Objects> recshippingreplacement = (Map<String, Objects>) shippingReplacementDao.selectRecycle(map1.get("replaceId"));
                                    Map<String, Objects> shippingreplacement = (Map<String, Objects>) shippingReplacementDao.select(map1.get("replaceId"));
                                    if (recshippingreplacement == null) {
                                        if (shippingreplacement == null) {
                                            try {
                                                shippingReplacementDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(shippingreplacement.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    shippingReplacementDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recshippingreplacement.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                shippingReplacementDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{siteMessage}siteMessage":
                                    Map<String, Objects> recsitemsg = (Map<String, Objects>) siteMsgDao.selectRecycle(map1.get("uid"));
                                    Map<String, Objects> sitemsg = (Map<String, Objects>) siteMsgDao.select(map1.get("uid"));
                                    if (recsitemsg == null) {
                                        if (sitemsg == null) {
                                            try {
                                                siteMsgDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(sitemsg.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    siteMsgDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recsitemsg.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                siteMsgDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{siteMessage}siteMessage:read":
                                    Map<String, Objects> recsitemsgread = (Map<String, Objects>) siteMsgReadDao.selectRecycle(map1.get("uid"));
                                    Map<String, Objects> sitemsgread = (Map<String, Objects>) siteMsgReadDao.select(map1.get("uid"));
                                    if (recsitemsgread == null) {
                                        if (sitemsgread == null) {
                                            try {
                                                siteMsgReadDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(sitemsgread.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    siteMsgReadDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recsitemsgread.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                siteMsgReadDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{siteMessage}siteMessage:type":
                                    Map<String, Objects> recsitemsgtype = (Map<String, Objects>) siteMsgTypeDao.selectRecycle(map1.get("msgCode"));
                                    Map<String, Objects> sitemsgtype = (Map<String, Objects>) siteMsgTypeDao.select(map1.get("msgCode"));
                                    if (recsitemsgtype == null) {
                                        if (sitemsgtype == null) {
                                            try {
                                                siteMsgTypeDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(sitemsgtype.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    siteMsgTypeDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recsitemsgtype.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                siteMsgTypeDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{verification}SliderCaptcha":
//                                    Map<String, Objects> recstaff = (Map<String, Objects>) staffDao.selectRecycle(map1.get("employeeId"));
//                                    Map<String, Objects> staff = (Map<String, Objects>) staffDao.select(map1.get("employeeId"));
//                                    if (recstaff == null) {
//                                        if (staff == null) {
//                                            try {
//                                                staffDao.add(map1);
//                                            } catch (Exception e) {
//                                                kafkaproduce.kp(String.valueOf(map1));
//                                                System.out.println("操作失败，转发kafka");
//                                            }
//                                        } else {
//                                            if (Integer.parseInt(String.valueOf(staff.get("version"))) < Integer.parseInt(map1.get("version"))) {
//                                                try {
//                                                    staffDao.update(map1);
//                                                } catch (Exception e) {
//                                                    kafkaproduce.kp(String.valueOf(map1));
//                                                    System.out.println("操作失败，转发kafka");
//                                                }
//                                            } else {
//                                                System.out.println("过时数据，不进行操作。");
//                                            }
//                                        }
//                                    } else {
//                                        if (Integer.parseInt(String.valueOf(recstaff.get("version"))) > Integer.parseInt(map1.get("version"))) {
//                                            System.out.println("过时数据，不进行操作。");
//                                        } else {
//                                            try {
//                                                staffDao.add(map1);
//                                            } catch (Exception e) {
//                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
//                                                System.out.println("数据库操作失败，转发kafka");
//                                            }
//                                        }
//                                    }
                                    break;

                                case "hmall:cache:{sms}Sms":
                                    Map<String, Objects> recsms = (Map<String, Objects>) smsDao.selectRecycle(map1.get("uid"));
                                    Map<String, Objects> sms = (Map<String, Objects>) smsDao.select(map1.get("uid"));
                                    if (recsms == null) {
                                        if (sms == null) {
                                            try {
                                                smsDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(sms.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    smsDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recsms.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                smsDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{staff}Staff":
                                    Map<String, Objects> recstaff = (Map<String, Objects>) staffDao.selectRecycle(map1.get("employeeId"));
                                    Map<String, Objects> staff = (Map<String, Objects>) staffDao.select(map1.get("employeeId"));
                                    if (recstaff == null) {
                                        if (staff == null) {
                                            try {
                                                staffDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(staff.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    staffDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recstaff.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                staffDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{user}User:staffmobile":
                                    Map<String, Objects> recstaffmobile = (Map<String, Objects>) staffMobileDao.selectRecycle(map1.get("mobileNumber"));
                                    Map<String, Objects> staffmobile = (Map<String, Objects>) staffMobileDao.select(map1.get("mobileNumber"));
                                    if (recstaffmobile == null) {
                                        if (staffmobile == null) {
                                            try {
                                                staffMobileDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(staffmobile.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    staffMobileDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recstaffmobile.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                staffMobileDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{stock}Center":
                                    Map<String, Objects> recstockcenter = (Map<String, Objects>) stockCenterDao.selectRecycle(map1.get("uid"));
                                    Map<String, Objects> stockcenter = (Map<String, Objects>) stockCenterDao.select(map1.get("uid"));
                                    if (recstockcenter == null) {
                                        if (stockcenter == null) {
                                            try {
                                                stockCenterDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(stockcenter.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    stockCenterDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recstockcenter.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                stockCenterDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{stock}stock":
                                    Map<String, Objects> recstock = (Map<String, Objects>) stockDao.selectRecycle(map1.get("stockId"));
                                    Map<String, Objects> stock = (Map<String, Objects>) stockDao.select(map1.get("stockId"));
                                    if (recstock == null) {
                                        if (stock == null) {
                                            try {
                                                stockDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(stock.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    stockDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recstock.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                stockDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{stock}Store":
                                    Map<String, Objects> recstockstore = (Map<String, Objects>) stockStoreDao.selectRecycle(map1.get("uid"));
                                    Map<String, Objects> stockstore = (Map<String, Objects>) stockStoreDao.select(map1.get("uid"));
                                    if (recstockstore == null) {
                                        if (stockstore == null) {
                                            try {
                                                stockStoreDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(stockstore.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    stockStoreDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recstockstore.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                stockStoreDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{base}Base:area":
                                    Map<String, Objects> rectaoarea = (Map<String, Objects>) taoAreaDao.selectRecycle(map1.get("id"));
                                    Map<String, Objects> taoarea = (Map<String, Objects>) taoAreaDao.select(map1.get("id"));
                                    if (rectaoarea == null) {
                                        if (taoarea == null) {
                                            try {
                                                taoAreaDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(taoarea.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    taoAreaDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(rectaoarea.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                taoAreaDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{thirdParty}thirdParty:taobao":
                                    Map<String, Objects> rectaobao = (Map<String, Objects>) taobaoDao.selectRecycle(map1.get("taobaoOpenId"));
                                    Map<String, Objects> taobao = (Map<String, Objects>) taobaoDao.select(map1.get("taobaoOpenId"));
                                    if (rectaobao == null) {
                                        if (taobao == null) {
                                            try {
                                                taobaoDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(taobao.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    taobaoDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(rectaobao.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                taobaoDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{test}Test":
                                    Map<String, Objects> rectest = (Map<String, Objects>) testDao.selectRecycle(map1.get("testId"));
                                    Map<String, Objects> test = (Map<String, Objects>) testDao.select(map1.get("testId"));
                                    if (rectest == null) {
                                        if (test == null) {
                                            try {
                                                testDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(test.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    testDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(rectest.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                testDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{thirdParty}thirdParty":
                                    Map<String, Objects> recthirdparty = (Map<String, Objects>) thirdPartyDao.selectRecycle(map1.get("uid"));
                                    Map<String, Objects> thirdparty = (Map<String, Objects>) thirdPartyDao.select(map1.get("uid"));
                                    if (recthirdparty == null) {
                                        if (thirdparty == null) {
                                            try {
                                                thirdPartyDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(thirdparty.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    thirdPartyDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recthirdparty.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                thirdPartyDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{user}User:address":
                                    Map<String, Objects> recuseraddress = (Map<String, Objects>) userAddressDao.selectRecycle(map1.get("addressId"));
                                    Map<String, Objects> useraddress = (Map<String, Objects>) userAddressDao.select(map1.get("addressId"));
                                    if (recuseraddress == null) {
                                        if (useraddress == null) {
                                            try {
                                                userAddressDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(useraddress.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    userAddressDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recuseraddress.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                userAddressDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{user}User":
                                    Map<String, Objects> recuser = (Map<String, Objects>) userDao.selectRecycle(map1.get("userId"));
                                    Map<String, Objects> user = (Map<String, Objects>) userDao.select(map1.get("userId"));
                                    if (recuser == null) {
                                        if (user == null) {
                                            try {
                                                userDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(user.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    userDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recuser.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                userDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{user}User:favorite":
                                    Map<String, Objects> recuserfavorite = (Map<String, Objects>) userFavoriteDao.selectRecycle(map1.get("uid"));
                                    Map<String, Objects> userfavorite = (Map<String, Objects>) userFavoriteDao.select(map1.get("uid"));
                                    if (recuserfavorite == null) {
                                        if (userfavorite == null) {
                                            try {
                                                userFavoriteDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(userfavorite.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    userFavoriteDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recuserfavorite.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                userFavoriteDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{user}User:info":
                                    Map<String, Objects> recuserinfo = (Map<String, Objects>) userInfoDao.selectRecycle(map1.get("userId"));
                                    Map<String, Objects> userinfo = (Map<String, Objects>) userInfoDao.select(map1.get("userId"));
                                    if (recuserinfo == null) {
                                        if (userinfo == null) {
                                            try {
                                                userInfoDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(userinfo.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    userInfoDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recuserinfo.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                userInfoDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{thirdParty}thirdParty:weibo":
                                    Map<String, Objects> recweibo = (Map<String, Objects>) weiboDao.selectRecycle(map1.get("weiboOpenId"));
                                    Map<String, Objects> weibo = (Map<String, Objects>) weiboDao.select(map1.get("weiboOpenId"));
                                    if (recweibo == null) {
                                        if (weibo == null) {
                                            try {
                                                weiboDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(weibo.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    weiboDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recweibo.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                weiboDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{thirdParty}thirdParty:weixin":
                                    Map<String, Objects> recweixi = (Map<String, Objects>) weixiDao.selectRecycle(map1.get("weixinOpenId"));
                                    Map<String, Objects> weixi = (Map<String, Objects>) weixiDao.select(map1.get("weixinOpenId"));
                                    if (recweixi == null) {
                                        if (weixi == null) {
                                            try {
                                                weixiDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(weixi.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    weixiDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(recweixi.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                weixiDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{base}Base:zoneDeliveryMode":
                                    Map<String, Objects> reczonedeliverymode= (Map<String, Objects>) zoneDeliveryModeDao.selectRecycle(map1.get("code"));
                                    Map<String, Objects> zonedeliverymode = (Map<String, Objects>) zoneDeliveryModeDao.select(map1.get("code"));
                                    if (reczonedeliverymode == null) {
                                        if (zonedeliverymode == null) {
                                            try {
                                                zoneDeliveryModeDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(zonedeliverymode.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    zoneDeliveryModeDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(reczonedeliverymode.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                zoneDeliveryModeDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;

                                case "hmall:cache:{base}Base:zoneDeliveryModeValue":
                                    Map<String, Objects> reczonedeliverymodevalue = (Map<String, Objects>) zoneDeliveryModeValueDao.selectRecycle(map1.get("zoneDeliveryModeValueId"));
                                    Map<String, Objects> zonedeliverymodevalue = (Map<String, Objects>) zoneDeliveryModeValueDao.select(map1.get("zoneDeliveryModeValueId"));
                                    if (reczonedeliverymodevalue == null) {
                                        if (zonedeliverymodevalue == null) {
                                            try {
                                                zoneDeliveryModeValueDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp(String.valueOf(map1));
                                                System.out.println("操作失败，转发kafka");
                                            }
                                        } else {
                                            if (Integer.parseInt(String.valueOf(zonedeliverymodevalue.get("version"))) < Integer.parseInt(map1.get("version"))) {
                                                try {
                                                    zoneDeliveryModeValueDao.update(map1);
                                                } catch (Exception e) {
                                                    kafkaproduce.kp(String.valueOf(map1));
                                                    System.out.println("操作失败，转发kafka");
                                                }
                                            } else {
                                                System.out.println("过时数据，不进行操作。");
                                            }
                                        }
                                    } else {
                                        if (Integer.parseInt(String.valueOf(reczonedeliverymodevalue.get("version"))) > Integer.parseInt(map1.get("version"))) {
                                            System.out.println("过时数据，不进行操作。");
                                        } else {
                                            try {
                                                zoneDeliveryModeValueDao.add(map1);
                                            } catch (Exception e) {
                                                kafkaproduce.kp("数据库操作失败，转发kafka" + String.valueOf(map1));
                                                System.out.println("数据库操作失败，转发kafka");
                                            }
                                        }
                                    }
                                    break;
                            }
                        }
                    }
                }
            }
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
    }

    public Boolean getRunning() {
        return running;
    }

    public void setRunning(Boolean running) {
        this.running = running;
    }
}
