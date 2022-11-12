package com.github.liuche51.easyTaskX.exception;

/**
 *  EasyTask通用异常
 */
public class EasyTaskException extends Exception{
    public EasyTaskException(String code,String message){
        super(code+":"+message);
    }
}
