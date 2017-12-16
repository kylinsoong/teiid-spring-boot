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
package org.teiid.spring.odata;

import java.io.File;
import java.io.IOException;
import java.lang.ref.SoftReference;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.olingo.server.api.ODataHttpHandler;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.VDB;
import org.teiid.core.TeiidProcessingException;
import org.teiid.metadata.Schema;
import org.teiid.odata.api.Client;
import org.teiid.olingo.service.OlingoBridge;
import org.teiid.olingo.web.ContextAwareHttpSerlvetRequest;
import org.teiid.olingo.web.ODataFilter;
import org.teiid.spring.autoconfigure.TeiidConstants;
import org.teiid.spring.autoconfigure.TeiidServer;
import org.teiid.vdb.runtime.VDBKey;

public class SpringODataFilter extends ODataFilter {

	private TeiidServer server;
	
	private static final String ODATA4_PRIFIX = File.separator + "odata4" + File.separator;
	
	private String vdbName = null;
	private String vdbVersion = null;
	
	public SpringODataFilter(TeiidServer server) {
		this.server = server;
	}
	
	@Override
    public void internalDoFilter(ServletRequest request, ServletResponse response,
            FilterChain chain) throws IOException, ServletException, TeiidProcessingException {

        HttpServletRequest httpRequest = (HttpServletRequest) request;

        String uri = ((HttpServletRequest) request).getRequestURI().toString();
        String fullURL = ((HttpServletRequest) request).getRequestURL().toString();
        if (uri.contains("/static/") || uri.contains("/keycloak/")){ //$NON-NLS-1$ //$NON-NLS-2$
            chain.doFilter(httpRequest, response);
            return;
        }
        
        String contextPath = httpRequest.getContextPath();
        String baseURI = fullURL.substring(0, fullURL.indexOf(contextPath));

        ContextAwareHttpSerlvetRequest contextAwareRequest = new ContextAwareHttpSerlvetRequest(httpRequest);
        contextAwareRequest.setContextPath(contextPath);
        httpRequest = contextAwareRequest;
        
        VDBKey key = buildVDBKey(uri);
        
        SoftReference<OlingoBridge> ref = this.contextMap.get(key);
        OlingoBridge context = null;
        if (ref != null) {
            context = ref.get();
        }
        
        if (context == null) {
            context = new OlingoBridge();
            ref = new SoftReference<OlingoBridge>(context);
            this.contextMap.put(key, ref);
        }
        
        Client client = buildClient(key.getName(), key.getVersion(), this.initProperties);
        try {
            Connection connection = client.open();
            registerVDBListener(client, connection);
            ODataHttpHandler handler = context.getHandler(baseURI, client, modelName());
            httpRequest.setAttribute(ODataHttpHandler.class.getName(), handler);
            httpRequest.setAttribute(Client.class.getName(), client);
            chain.doFilter(httpRequest, response);
        } catch(SQLException e) {
            throw new TeiidProcessingException(e);
        } finally {
            try {
                client.close();
            } catch (SQLException e) {
                //ignore
            }
        }
    }
    
	private VDBKey buildVDBKey(String uri) {
		if(uri.startsWith(ODATA4_PRIFIX)) {
			String path = uri.substring(ODATA4_PRIFIX.length());
			this.vdbName = path.substring(0, path.indexOf(File.separator));
			this.vdbVersion = "1";
		} else {
			this.vdbName = TeiidConstants.VDBNAME;
			this.vdbVersion = TeiidConstants.VDBVERSION;
		}
		return new VDBKey(vdbName, vdbVersion);
	}

	public String modelName() {
		Schema schema = server.getSchema("teiid");
		if (schema != null && !schema.getTables().isEmpty()) {
			return "teiid";
		}

		String modelName = null;
		try {						
			VDB vdb = server.getAdmin().getVDB(vdbName, vdbVersion);			
			for (Model m : vdb.getModels()) {
				if (m.getName().equals("file") || m.getName().equals("rest") || m.getName().equals("teiid")) {
					continue;
				}
				modelName = m.getName();
				break;
			}
		} catch (AdminException e) {
		}
		return modelName;
	}
	
    public Client buildClient(String vdbName, String version, Properties props) {
        return new SpringClient(vdbName, version, props, server);        
    }
}
