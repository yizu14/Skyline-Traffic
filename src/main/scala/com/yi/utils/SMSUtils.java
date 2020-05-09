package com.yi.utils;



import com.tencentcloudapi.common.Credential;
import com.tencentcloudapi.common.exception.TencentCloudSDKException;
import com.tencentcloudapi.sms.v20190711.SmsClient;
import com.tencentcloudapi.sms.v20190711.models.SendSmsRequest;
import com.tencentcloudapi.sms.v20190711.models.SendSmsResponse;

import java.io.IOException;
import java.util.ArrayList;

public class SMSUtils {
    private static String secret_id = "AKIDFC8vYzPJvPp34tDN4MYTL5r7ACoDgSJz";
    private static String secret_key = "PeAf2DCVdXBi2HG0jeJk4AIE6vb5S3XB";
    private static String sms_app_id = "1400308849";
    private static String sms_sign = "Sin的专属空间";
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
