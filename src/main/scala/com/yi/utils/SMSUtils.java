package com.yi.utils;



import com.tencentcloudapi.common.Credential;
import com.tencentcloudapi.common.exception.TencentCloudSDKException;
import com.tencentcloudapi.sms.v20190711.SmsClient;
import com.tencentcloudapi.sms.v20190711.models.SendSmsRequest;
import com.tencentcloudapi.sms.v20190711.models.SendSmsResponse;

import java.io.IOException;
import java.util.ArrayList;

public class SMSUtils {
    //Tencent Cloud Properties(Already Hidden)
    private static String secret_id = "XX";
    private static String secret_key = "XX";
    private static String sms_app_id = "XX";
    private static String sms_sign = "XX";
    private static String sms_template_id = "587772";
    public static void send(String plate,String time,String blockid,String roadid){
        String[] params = {plate,time,blockid,roadid};
        String[] phoneNumbers = {"+8617519549918"};
        Credential cred = new Credential(secret_id, secret_key);
        SmsClient client = new SmsClient(cred, "");
        SendSmsRequest req = new SendSmsRequest();
        req.setSmsSdkAppid(sms_app_id);
        req.setSign(sms_sign);
        req.setTemplateID(sms_template_id);
        req.setTemplateParamSet(params);
        req.setPhoneNumberSet(phoneNumbers);
        try {
            SendSmsResponse sendSmsResponse = client.SendSms(req);
            System.err.println(SendSmsResponse.toJsonString(sendSmsResponse));
        } catch (TencentCloudSDKException e) {
            e.printStackTrace();
        }
    }
}
