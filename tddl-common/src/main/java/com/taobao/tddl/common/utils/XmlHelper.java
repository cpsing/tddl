package com.taobao.tddl.common.utils;

import java.io.InputStream;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.w3c.dom.Document;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import com.taobao.tddl.common.exception.TddlRuntimeException;

/**
 * xml处理的一些简单包装
 * 
 * @author jianghang 2013-11-28 下午1:59:37
 * @since 5.1.0
 */
public class XmlHelper {

    public static Document createDocument(InputStream xml, InputStream schema) {
        try {
            // schema
            SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            Schema s = sf.newSchema(new StreamSource(schema));
            // document
            DocumentBuilderFactory bf = DocumentBuilderFactory.newInstance();
            bf.setNamespaceAware(true);
            bf.setSchema(s);
            DocumentBuilder builder = bf.newDocumentBuilder();
            builder.setErrorHandler(new ErrorHandler() {

                @Override
                public void warning(SAXParseException exception) throws SAXException {
                    throw new TddlRuntimeException("Xml Parser Warning.", exception);
                }

                @Override
                public void fatalError(SAXParseException exception) throws SAXException {
                    throw new TddlRuntimeException("Xml Parser Fetal Error.", exception);
                }

                @Override
                public void error(SAXParseException exception) throws SAXException {
                    throw new TddlRuntimeException("Xml Parser Error.", exception);
                }
            });
            return builder.parse(xml);
        } catch (Exception e) {
            throw new TddlRuntimeException("Xml Parser Error.", e);
        }
    }

}
