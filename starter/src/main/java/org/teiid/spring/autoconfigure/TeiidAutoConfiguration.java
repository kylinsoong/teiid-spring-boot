/*
 * Copyright 2012-2017 the original author or authors.
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Driver;
import java.util.List;

import javax.sql.DataSource;
import javax.transaction.TransactionManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.boot.model.naming.PhysicalNamingStrategy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;
import org.springframework.core.Ordered;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;
import org.springframework.jdbc.datasource.embedded.ConnectionProperties;
import org.springframework.jdbc.datasource.embedded.DataSourceFactory;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseFactory;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.deployers.VDBRepository;
import org.teiid.metadatastore.DeploymentBasedDatabaseStore;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.spring.autoconfigure.TeiidPostProcessor.Registrar;
import org.teiid.spring.data.file.FileConnectionFactory;
import org.teiid.translator.ExecutionFactory;

@Configuration
@ConditionalOnClass({EmbeddedServer.class, ExecutionFactory.class})
@EnableConfigurationProperties(TeiidProperties.class)
@Import({ Registrar.class })
@PropertySource("classpath:teiid.properties")
public class TeiidAutoConfiguration implements Ordered {
    
    public static ThreadLocal<TeiidServer> serverContext = new ThreadLocal<>(); 
    
    private static final Log logger = LogFactory.getLog(TeiidAutoConfiguration.class);    
        
    @Autowired(required = false)
    private EmbeddedConfiguration embeddedConfiguration;
    
    @Autowired
    private TeiidProperties properties;
    
    @Autowired
    ApplicationContext context;
    
    @Value("${spring.jpa.hibernate.naming.physical-strategy:org.springframework.boot.orm.jpa.hibernate.SpringPhysicalNamingStrategy}")
    String hibernateNamingClass;
    
    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }
    
    @Bean
    @ConditionalOnMissingBean
    public TeiidInitializer teiidInitializer(ApplicationContext applicationContext) {
        return new TeiidInitializer(applicationContext);
    }
    
    @Bean
    @ConditionalOnMissingBean
    public VDBMetaData teiidVDB() {
        List<Resource> resources = TeiidInitializer.getScripts("teiid.vdb-file", this.properties.getVdbFile(),
                "teiid.ddl", this.context);
        
        VDBMetaData vdb = null;
        if (!resources.isEmpty()) {
            try {
                DeploymentBasedDatabaseStore store = new DeploymentBasedDatabaseStore(new VDBRepository());
                String db = "CREATE DATABASE "+VDBNAME+" VERSION '"+VDBVERSION+"';\n";
                db = db + "USE DATABASE "+VDBNAME+" VERSION '"+VDBVERSION+"';\n";
                db = db + ObjectConverterUtil.convertToString(resources.get(0).getInputStream());
                vdb = store.getVDBMetadata(db);
                logger.info("Predefined VDB found :" + resources.get(0).getFilename());
            } catch (FileNotFoundException e) {
                // no-op
            } catch (IOException e) {
                throw new IllegalStateException("Failed to parse the VDB defined");
            }
        }

        if (vdb == null) {
            vdb =  new VDBMetaData();
            vdb.addProperty("implicit", "true");
        }
        vdb.setName(VDBNAME);
        vdb.setVersion(VDBVERSION);            
        return vdb;
    }    
    
    @Bean(name = "teiid")
    @ConditionalOnMissingBean
    @Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
    public TeiidServer teiidServer(TransactionManager transactionManager) {
        logger.info("Starting Teiid Server.");
        
        // turning off PostgreSQL support
        System.setProperty("org.teiid.addPGMetadata", "false");
        
        final TeiidServer server = new TeiidServer();
        
        if(embeddedConfiguration == null) {
            embeddedConfiguration = new EmbeddedConfiguration();
            embeddedConfiguration.setTransactionManager(transactionManager);
        }
        
        if(embeddedConfiguration.getTransactionManager() == null) {
        	embeddedConfiguration.setTransactionManager(transactionManager);
        }
        
        if(embeddedConfiguration.getTransports() != null) {
        	embeddedConfiguration.getTransports().forEach(s -> {
        		logger.info("Teiid transport listen on " + s.getHostName() + ":" + s.getPortNumber());
        	});
        }
        
        server.start(embeddedConfiguration);
                       
        // this is dummy vdb to satisfy the boot process to create the connections
        VDBMetaData vdb =  new VDBMetaData();
        vdb.setName(VDBNAME);
        vdb.setVersion(VDBVERSION);                    
        server.deployVDB(vdb);        
        
        serverContext.set(server);
        return server;
    }
     
    @Bean(name="dataSource")
    @Primary
    @ConfigurationProperties(prefix = "spring.datasource")
    public DataSource dataSource(TeiidServer server) {
        EmbeddedDatabaseFactory edf = new EmbeddedDatabaseFactory();
        edf.setDatabaseConfigurer(new TeiidDatabaseConfigurer(server));
        edf.setDataSourceFactory(new DataSourceFactory() {
			@Override
			public DataSource getDataSource() {
				String url = context.getEnvironment().getProperty("spring.datasource.teiid.url");
				return new SimpleDriverDataSource(server.getDriver(), url);
			}
			
			@Override
			public ConnectionProperties getConnectionProperties() {
				return new ConnectionProperties() {
					@Override
					public void setDriverClass(Class<? extends Driver> driverClass) {
					}
					@Override
					public void setUrl(String url) {
					}
					@Override
					public void setUsername(String username) {
					}
					@Override
					public void setPassword(String password) {
					}
				};
			}
		});
        return edf.getDatabase();
    }    
    
    @Bean(name="file")
    public FileConnectionFactory fileConnectionFactory() {
        return new FileConnectionFactory();
    }
    
    @Bean(name="teiidNamingStrategy")
    public PhysicalNamingStrategy teiidNamingStrategy() {
    	try {
    		return (PhysicalNamingStrategy)Class.forName(hibernateNamingClass).newInstance();
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			return null;
		}
	}
}
