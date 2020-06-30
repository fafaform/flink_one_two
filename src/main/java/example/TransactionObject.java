package example;

import java.io.UnsupportedEncodingException;

public class TransactionObject {
    private long TXN_ID;
    private long TIMESTAMP;
    private String CARD_TYPE;
    private String CARD_STATUS;
    private double TXN_AMT;
    private String CARD_NUMBER;

    public TransactionObject(){}

    public long getTXN_ID() {
        return TXN_ID;
    }

    public void setTXN_ID(byte[] TXN_ID) {
        String unpack = unpackData(TXN_ID);
        this.TXN_ID = Long.parseLong(unpack);
    }

    public long getTIMESTAMP() {
        return TIMESTAMP;
    }

    public void setTIMESTAMP(byte[] TIMESTAMP) {
        String unpack = unpackData(TIMESTAMP);
        this.TIMESTAMP = Long.parseLong(unpack);
    }

    public String getCARD_TYPE() {
        return CARD_TYPE;
    }

    public void setCARD_TYPE(byte[] CARD_TYPE) throws UnsupportedEncodingException {
        this.CARD_TYPE = new String(CARD_TYPE, "CP1047");;
    }

    public String getCARD_STATUS() {
        return CARD_STATUS;
    }

    public void setCARD_STATUS(byte[] CARD_STATUS) throws UnsupportedEncodingException {
        this.CARD_STATUS = new String(CARD_STATUS, "CP1047");
    }

    public double getTXN_AMT() {
        return TXN_AMT;
    }

    public void setTXN_AMT(byte[] TXN_AMT) {
        String unpack = unpackData(TXN_AMT);
        this.TXN_AMT = Double.parseDouble(unpack)/100;
    }

    public String getCARD_NUMBER() {
        return CARD_NUMBER;
    }

    public void setCARD_NUMBER(byte[] CARD_NUMBER) throws UnsupportedEncodingException {
        this.CARD_NUMBER = new String(CARD_NUMBER, "CP1047");
    }

    private static String unpackData(byte[] packedData) {
        String unpackedData = "";

        final int negativeSign = 13;
        for (int currentCharIndex = 0; currentCharIndex < packedData.length; currentCharIndex++) {
            byte firstDigit = (byte) ((packedData[currentCharIndex] >>> 4) & 0x0F);
            byte secondDigit = (byte) (packedData[currentCharIndex] & 0x0F);
            unpackedData += String.valueOf(firstDigit);
            if (currentCharIndex == (packedData.length - 1)) {
                if (secondDigit == negativeSign) {
                    unpackedData = "-" + unpackedData;
                }
            } else {
                unpackedData += String.valueOf(secondDigit);
            }
        }
//        System.out.println("Unpackeddata is :" + unpackedData);

        return unpackedData;
    }

}
