/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;


public class GossipingPropertyFileSnitch extends AbstractNetworkTopologySnitch// implements IEndpointStateChangeSubscriber
{
    private static final Logger logger = LoggerFactory.getLogger(GossipingPropertyFileSnitch.class);

    public static final String PFS_COMPAT_MODE_PROP = "cassandra.gpfs.enable_pfs_compatibility_mode";
    public static Boolean PFS_COMPATIBILITY_ENABLED = Boolean.valueOf(System.getProperty(PFS_COMPAT_MODE_PROP, "false"));

    private PropertyFileSnitch psnitch;

    private final String myDC;
    private final String myRack;
    private final boolean preferLocal;
    private final AtomicReference<ReconnectableSnitchHelper> snitchHelperReference;

    private Map<InetAddress, Map<String, String>> savedEndpoints;
    protected static final String DEFAULT_DC = "UNKNOWN_DC";
    protected static final String DEFAULT_RACK = "UNKNOWN_RACK";

    public GossipingPropertyFileSnitch() throws ConfigurationException
    {
        this(PFS_COMPATIBILITY_ENABLED);
    }

    public GossipingPropertyFileSnitch(boolean enablePfsCompatibilityMode) throws ConfigurationException
    {
        SnitchProperties properties = loadConfiguration();

        myDC = properties.get("dc", DEFAULT_DC).trim();
        myRack = properties.get("rack", DEFAULT_RACK).trim();
        preferLocal = Boolean.parseBoolean(properties.get("prefer_local", "false"));
        snitchHelperReference = new AtomicReference<>();

        if (enablePfsCompatibilityMode)
        {
            try
            {
                psnitch = new PropertyFileSnitch();
                logger.info("Loaded {} for compatibility", PropertyFileSnitch.SNITCH_PROPERTIES_FILENAME);
            }
            catch (ConfigurationException e)
            {
                logger.info("Unable to load {}; compatibility mode disabled", PropertyFileSnitch.SNITCH_PROPERTIES_FILENAME);
            }
        }
        else
        {
            logger.debug("Property file snitch compatibility mode is disabled. Set startup property {}=true to enable.", PFS_COMPAT_MODE_PROP);
        }
    }

    private static SnitchProperties loadConfiguration() throws ConfigurationException
    {
        final SnitchProperties properties = new SnitchProperties();
        if (!properties.contains("dc") || !properties.contains("rack"))
            throw new ConfigurationException("DC or rack not found in snitch properties, check your configuration in: " + SnitchProperties.RACKDC_PROPERTY_FILENAME);

        return properties;
    }

    /**
     * Return the data center for which an endpoint resides in
     *
     * @param endpoint the endpoint to process
     * @return string of data center
     */
    public String getDatacenter(InetAddress endpoint)
    {
        if (endpoint.equals(FBUtilities.getBroadcastAddress()))
            return myDC;

        EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        VersionedValue dc = epState != null ? epState.getApplicationState(ApplicationState.DC) : null;
        if (dc != null)
            return dc.value;

        if (savedEndpoints == null)
            savedEndpoints = SystemKeyspace.loadDcRackInfo();
        if (savedEndpoints.containsKey(endpoint))
            return savedEndpoints.get(endpoint).get("data_center");

        if (psnitch != null)
            return psnitch.getDatacenter(endpoint);

        return DEFAULT_DC;
    }

    /**
     * Return the rack for which an endpoint resides in
     *
     * @param endpoint the endpoint to process
     * @return string of rack
     */
    public String getRack(InetAddress endpoint)
    {
        if (endpoint.equals(FBUtilities.getBroadcastAddress()))
            return myRack;

        EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        VersionedValue rack = epState != null ? epState.getApplicationState(ApplicationState.RACK) : null;
        if (rack != null)
            return rack.value;

        if (savedEndpoints == null)
            savedEndpoints = SystemKeyspace.loadDcRackInfo();
        if (savedEndpoints.containsKey(endpoint))
            return savedEndpoints.get(endpoint).get("rack");

        if (psnitch != null)
            return psnitch.getRack(endpoint);

        return DEFAULT_RACK;
    }

    public void gossiperStarting()
    {
        super.gossiperStarting();

        Gossiper.instance.addLocalApplicationState(ApplicationState.INTERNAL_IP,
                StorageService.instance.valueFactory.internalIP(FBUtilities.getLocalAddress().getHostAddress()));

        loadGossiperState();
    }

    private void loadGossiperState()
    {
        assert Gossiper.instance != null;

        ReconnectableSnitchHelper pendingHelper = new ReconnectableSnitchHelper(this, myDC, preferLocal);
        Gossiper.instance.register(pendingHelper);

        pendingHelper = snitchHelperReference.getAndSet(pendingHelper);
        if (pendingHelper != null)
            Gossiper.instance.unregister(pendingHelper);
    }

    @Override
    public boolean isDefaultDC(String dc)
    {
        assert dc != null;
        return dc == DEFAULT_DC;
    }

    public String toString()
    {
        return "GossipingPropertyFileSnitch{" + "psnitch=" + psnitch +
               ", myDC='" + myDC + '\'' +
               ", myRack='" + myRack + '\'' +
               ", preferLocal=" + preferLocal +
               ", snitchHelperReference=" + snitchHelperReference +
               '}';
    }
}
