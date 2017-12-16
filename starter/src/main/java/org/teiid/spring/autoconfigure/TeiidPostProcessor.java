/*
 * Copyright 2012-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.spring.autoconfigure;

import static org.teiid.spring.autoconfigure.TeiidConstants.VDBNAME;
import static org.teiid.spring.autoconfigure.TeiidConstants.VDBVERSION;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

import javax.sql.DataSource;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLStreamException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.boot.model.naming.PhysicalNamingStrategy;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.Ordered;
import org.springframework.core.type.AnnotationMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBMetadataParser;
import org.teiid.spring.data.BaseConnectionFactory;
import org.teiid.translator.ExecutionFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * {@link BeanPostProcessor} used to fire {@link TeiidInitializedEvent}s. Should only
 * be registered via the inner {@link Registrar} class.
 *
 * Code as template taken from {@link DataSourceInitializerPostProcessor}
 */
class TeiidPostProcessor implements BeanPostProcessor, Ordered, ApplicationListener<ContextRefreshedEvent>{
    private static final Log logger = LogFactory.getLog(TeiidPostProcessor.class);
    
	@Override
	public int getOrder() {
		return Ordered.HIGHEST_PRECEDENCE;
	}
	
	@Autowired
	private BeanFactory beanFactory;
	
	@Autowired
	private ApplicationContext context;
	
	@Autowired
	private PhysicalNamingStrategy namingStrategy;
	
	@Override
	public Object postProcessBeforeInitialization(Object bean, String beanName)
			throws BeansException {
	    if (bean instanceof DataSource) {
	        this.beanFactory.getBean("teiid", TeiidServer.class);
	    }
		return bean;
	}

	@Override
	public Object postProcessAfterInitialization(Object bean, String beanName)
			throws BeansException {
		if (bean instanceof TeiidServer) {
			// force initialization of this bean as soon as we see a TeiidServer
			this.beanFactory.getBean(TeiidInitializer.class);
		} else if (bean instanceof DataSource && !beanName.equals("dataSource")) {
		    // initialize databases if any
		    new MultiDataSourceInitializer((DataSource)bean, beanName, context).init();
		    
		    TeiidServer server = this.beanFactory.getBean(TeiidServer.class);		    
		    VDBMetaData vdb = this.beanFactory.getBean(VDBMetaData.class);
		    server.addDataSource(vdb, beanName, (DataSource)bean, context);		        		
		    logger.info("Datasource added to Teiid = " + beanName);
		} else if (bean instanceof BaseConnectionFactory) {
            TeiidServer server = this.beanFactory.getBean(TeiidServer.class);           
            VDBMetaData vdb = this.beanFactory.getBean(VDBMetaData.class);
            server.addDataSource(vdb, beanName, (BaseConnectionFactory)bean, context);                     
            logger.info("Non JDBC Datasource added to Teiid = " + beanName);
		} else if (bean instanceof ExecutionFactory) {
			TeiidServer server = this.beanFactory.getBean(TeiidServer.class);
			ExecutionFactory<?,?> ef = this.beanFactory.getBean(ExecutionFactory.class);
			server.addTranslator(beanName, ef);
			logger.info("Teiid translator \"" + beanName + "\" added.");
		}
		return bean;
	}

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        boolean deploy = true;
        VDBMetaData vdb = this.beanFactory.getBean(VDBMetaData.class);
        TeiidServer server = this.beanFactory.getBean(TeiidServer.class);
        if (vdb.getPropertyValue("implicit") != null && vdb.getPropertyValue("implicit").equals("true")) {
            deploy = server.findAndConfigureViews(vdb, event.getApplicationContext(), namingStrategy);
        }

        if (deploy) {
        	try {
	            // Deploy at the end when all the data sources are configured
	            server.undeployVDB(VDBNAME, VDBVERSION);
	            ByteArrayOutputStream out = new ByteArrayOutputStream();
	            VDBMetadataParser.marshell(vdb, out);
	            logger.debug("XML Form of VDB:\n" + prettyFormat(new String(out.toByteArray())));	            
	            server.deployVDB(vdb);
			} catch ( IOException | XMLStreamException e) {
				// no-op
			}            
        }
    }	
    
    
    private String prettyFormat(String xml){
    	try {
			Transformer transformer = TransformerFactory.newInstance().newTransformer();
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
			StreamResult result = new StreamResult(new StringWriter());
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();    	
			InputSource is = new InputSource(new StringReader(xml));
			DOMSource source = new DOMSource(db.parse(is));
			transformer.transform(source, result);
			return result.getWriter().toString();
		} catch (IllegalArgumentException | TransformerFactoryConfigurationError | ParserConfigurationException
				| SAXException | IOException | TransformerException e) {
			return xml;
		}
    }
    
    /**
	 * {@link ImportBeanDefinitionRegistrar} to register the
	 * {@link TeiidPostProcessor} without causing early bean instantiation
	 * issues.
	 */
	static class Registrar implements ImportBeanDefinitionRegistrar {

		private static final String BEAN_NAME = "teiidInitializerPostProcessor";

		@Override
		public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata,
				BeanDefinitionRegistry registry) {
			if (!registry.containsBeanDefinition(BEAN_NAME)) {
				GenericBeanDefinition beanDefinition = new GenericBeanDefinition();
				beanDefinition.setBeanClass(TeiidPostProcessor.class);
				beanDefinition.setRole(BeanDefinition.ROLE_APPLICATION);
				// We don't need this one to be post processed otherwise it can cause a
				// cascade of bean instantiation that we would rather avoid.
				beanDefinition.setSynthetic(true);
				registry.registerBeanDefinition(BEAN_NAME, beanDefinition);
			}
		}
	}

}
