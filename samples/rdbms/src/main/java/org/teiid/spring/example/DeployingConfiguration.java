package org.teiid.spring.example;

import javax.sql.DataSource;
import javax.xml.stream.XMLStreamException;

import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBMetadataParser;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.mysql.MySQL5ExecutionFactory;
import org.teiid.transport.SocketConfiguration;
import org.teiid.transport.WireProtocol;

@Configuration
public class DeployingConfiguration {
	
	@ConfigurationProperties(prefix = "spring.datasource.US_Customers")
    @Bean(name = "US_Customers")
    public DataSource usCustomers() {
        return DataSourceBuilder.create().build();
    }
    
    @ConfigurationProperties(prefix = "spring.datasource.APAC_Customers")
    @Bean(name = "APAC_Customers")
    public DataSource apacCustomers() {
        return DataSourceBuilder.create().build();
    }
	
	@Bean
	public VDBMetaData cnpcTest() {
		String vdbName = "CNPCTest-vdb.xml";
		try {
			VDBMetaData vdb = VDBMetadataParser.unmarshell(DeployingConfiguration.class.getClassLoader().getResourceAsStream(vdbName));
			return vdb;
		} catch (XMLStreamException e) {
			throw new IllegalArgumentException(vdbName + " not valid", e);
		}
	}
	
	@Bean(name = "mysql5")
	public ExecutionFactory<?,?> mysql5() {
		
		try {
			MySQL5ExecutionFactory ef = new MySQL5ExecutionFactory();
			ef.setSupportsDirectQueryProcedure(true);
			ef.start();
			return ef;
		} catch (TranslatorException e) {
			throw new IllegalStateException("Teiid translator start error", e);
		}
	}
	
	@Bean
	public EmbeddedConfiguration configuration() {
		SocketConfiguration s = new SocketConfiguration();
		s.setBindAddress("0.0.0.0");
		s.setPortNumber(31000);
		s.setProtocol(WireProtocol.teiid);
		EmbeddedConfiguration config = new EmbeddedConfiguration();
		config.addTransport(s);
		return config;
	}
	

}
