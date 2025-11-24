import React, { useState } from 'react';
import { Search, User, Mail, Phone, ShoppingBag, X, TrendingUp, DollarSign, Users, ChevronRight, MapPin, MessageSquare, Send } from 'lucide-react';
import { LineChart, Line, BarChart, Bar, PieChart, Pie, Cell, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';

const mockDatabase = {
  locations: [
    { id: 'loc_001', name: 'Downtown Bellevue', address: '10300 Main St, Bellevue, WA' },
    { id: 'loc_002', name: 'Seattle Center', address: '305 Harrison St, Seattle, WA' },
    { id: 'loc_003', name: 'Redmond Town', address: '7325 166th Ave NE, Redmond, WA' }
  ],
  customers: [
    { id: 'cust_001', firstName: 'Katelyn', lastName: 'Wong', email: 'bhatsanaa23@gmail.com', phone: '+14257329471', loyaltyStatus: 'active', loyaltyPoints: 85, memberNumber: '8627422412274112', location: 'loc_001' },
    { id: 'cust_002', firstName: 'Sarah', lastName: 'Chen', email: 'sarah.chen@email.com', phone: '+14255551234', loyaltyStatus: 'active', loyaltyPoints: 120, memberNumber: '8627422412274113', location: 'loc_001' },
    { id: 'cust_003', firstName: 'Michael', lastName: 'Rodriguez', email: 'mrodriguez@email.com', phone: '+14255559876', loyaltyStatus: 'inactive', loyaltyPoints: 0, memberNumber: '8627422412274114', location: 'loc_002' },
    { id: 'cust_004', firstName: 'Emily', lastName: 'Johnson', email: 'emily.j@email.com', phone: '+14255553456', loyaltyStatus: 'active', loyaltyPoints: 250, memberNumber: '8627422412274115', location: 'loc_001' }
  ],
  orders: [
    { id: '#17', customerId: 'cust_001', amount: 48.87, tip: 8.31, tipPercent: 17, server: 'Shijie Feng', date: '2025-09-27', type: 'Dine-in', items: ['Pork Xiao Long Bao', 'Beef Stew Noodles'], location: 'loc_001' },
    { id: '#64', customerId: 'cust_001', amount: 48.36, tip: 6.77, tipPercent: 14, server: 'Diana T Nguyen', date: '2025-08-28', type: 'Dine-in', items: ['Pork Xiao Long Bao'], location: 'loc_001' },
    { id: '#65', customerId: 'cust_001', amount: 44.02, tip: 9.24, tipPercent: 21, server: 'TJ Eang', date: '2025-08-24', type: 'Dine-in', items: ['Beef Stew Noodles'], location: 'loc_001' },
    { id: '#29', customerId: 'cust_001', amount: 73.55, tip: 8.09, tipPercent: 11, server: 'Shijie Feng', date: '2025-08-23', type: 'Dine-in', items: ['Pork Xiao Long Bao'], location: 'loc_001' },
    { id: '#18', customerId: 'cust_002', amount: 62.40, tip: 12.48, tipPercent: 20, server: 'Shijie Feng', date: '2025-09-20', type: 'Dine-in', items: ['Spicy Wonton Soup'], location: 'loc_001' },
    { id: '#19', customerId: 'cust_004', amount: 95.20, tip: 19.04, tipPercent: 20, server: 'Diana T Nguyen', date: '2025-09-15', type: 'Dine-in', items: ['Xiao Long Bao'], location: 'loc_001' },
    { id: '#20', customerId: 'cust_003', amount: 55.30, tip: 8.30, tipPercent: 15, server: 'John Kim', date: '2025-09-10', type: 'Dine-in', items: ['Pan Fried Dumplings'], location: 'loc_002' }
  ]
};

const COLORS = ['#f97316', '#3b82f6', '#8b5cf6', '#10b981', '#ef4444', '#f59e0b'];

const DashboardApp = () => {
  const [activeTab, setActiveTab] = useState('performance');
  const [selectedLocation, setSelectedLocation] = useState('all');
  const [selectedCustomer, setSelectedCustomer] = useState(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [drilldownData, setDrilldownData] = useState(null);
  const [aiQuery, setAiQuery] = useState('');
  const [aiMessages, setAiMessages] = useState([]);
  const [isAiLoading, setIsAiLoading] = useState(false);

  const getFilteredOrders = () => {
    if (selectedLocation === 'all') return mockDatabase.orders;
    return mockDatabase.orders.filter(o => o.location === selectedLocation);
  };

  const getFilteredCustomers = () => {
    if (selectedLocation === 'all') return mockDatabase.customers;
    return mockDatabase.customers.filter(c => c.location === selectedLocation);
  };

  const calculatePerformance = () => {
    const orders = getFilteredOrders();
    const customers = getFilteredCustomers();
    
    const totalRevenue = orders.reduce((sum, o) => sum + o.amount, 0);
    const totalOrders = orders.length;
    const avgOrderValue = totalOrders > 0 ? totalRevenue / totalOrders : 0;
    const totalTips = orders.reduce((sum, o) => sum + o.tip, 0);
    const avgTip = orders.length > 0 ? orders.reduce((sum, o) => sum + o.tipPercent, 0) / orders.length : 0;
    
    const channelBreakdown = {};
    orders.forEach(o => {
      channelBreakdown[o.type] = (channelBreakdown[o.type] || 0) + 1;
    });

    const itemCounts = {};
    orders.forEach(order => {
      order.items.forEach(item => {
        itemCounts[item] = (itemCounts[item] || 0) + 1;
      });
    });
    const topItems = Object.entries(itemCounts).sort((a, b) => b[1] - a[1]).slice(0, 5);

    const customerSpend = {};
    orders.forEach(o => {
      customerSpend[o.customerId] = (customerSpend[o.customerId] || 0) + o.amount;
    });
    const topCustomers = Object.entries(customerSpend)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5)
      .map(([id, spend]) => {
        const customer = customers.find(c => c.id === id);
        return { customer, spend, orders: orders.filter(o => o.customerId === id).length };
      });

    const recentOrders = [...orders].sort((a, b) => new Date(b.date) - new Date(a.date)).slice(0, 10);

    // Revenue by date for chart
    const revenueByDate = {};
    orders.forEach(o => {
      const date = new Date(o.date).toLocaleDateString();
      revenueByDate[date] = (revenueByDate[date] || 0) + o.amount;
    });
    const revenueData = Object.entries(revenueByDate)
      .sort((a, b) => new Date(a[0]) - new Date(b[0]))
      .map(([date, revenue]) => ({ date, revenue }));

    // Location performance
    const locationPerformance = {};
    orders.forEach(o => {
      const loc = mockDatabase.locations.find(l => l.id === o.location);
      if (loc) {
        if (!locationPerformance[loc.name]) {
          locationPerformance[loc.name] = { revenue: 0, orders: 0 };
        }
        locationPerformance[loc.name].revenue += o.amount;
        locationPerformance[loc.name].orders += 1;
      }
    });
    const locationData = Object.entries(locationPerformance).map(([name, data]) => ({
      name,
      revenue: data.revenue,
      orders: data.orders
    }));

    return { 
      totalRevenue, 
      totalOrders, 
      avgOrderValue, 
      totalTips, 
      avgTip, 
      totalCustomers: customers.length, 
      loyaltyMembers: customers.filter(c => c.loyaltyStatus === 'active').length, 
      channelBreakdown, 
      topItems, 
      topCustomers, 
      recentOrders,
      revenueData,
      locationData
    };
  };

  const performance = calculatePerformance();

  const filteredCustomers = getFilteredCustomers().filter(c => 
    `${c.firstName} ${c.lastName}`.toLowerCase().includes(searchQuery.toLowerCase()) ||
    c.email.toLowerCase().includes(searchQuery.toLowerCase()) ||
    c.phone.includes(searchQuery)
  );

  const getCustomerMetrics = (customerId) => {
    const customerOrders = mockDatabase.orders.filter(o => o.customerId === customerId);
    const lifetimeSpend = customerOrders.reduce((sum, o) => sum + o.amount, 0);
    const avgSpend = customerOrders.length > 0 ? lifetimeSpend / customerOrders.length : 0;
    const avgTip = customerOrders.length > 0 ? customerOrders.reduce((sum, o) => sum + o.tipPercent, 0) / customerOrders.length : 0;
    const lastOrder = customerOrders.sort((a, b) => new Date(b.date) - new Date(a.date))[0]?.date;
    const firstOrder = customerOrders.sort((a, b) => new Date(a.date) - new Date(b.date))[0]?.date;
    
    const itemCounts = {};
    customerOrders.forEach(order => {
      order.items.forEach(item => {
        itemCounts[item] = (itemCounts[item] || 0) + 1;
      });
    });
    const mostOrderedItems = Object.entries(itemCounts).sort((a, b) => b[1] - a[1]).slice(0, 3);

    // Calculate days since last order
    const daysSinceLastOrder = lastOrder ? Math.floor((new Date() - new Date(lastOrder)) / (1000 * 60 * 60 * 24)) : null;
    
    // Calculate order channel breakdown
    const channelCounts = {};
    customerOrders.forEach(o => {
      channelCounts[o.type] = (channelCounts[o.type] || 0) + 1;
    });
    const totalChannelOrders = customerOrders.length;
    const channelBreakdown = Object.entries(channelCounts).map(([type, count]) => ({
      type,
      count,
      percentage: ((count / totalChannelOrders) * 100).toFixed(0)
    }));

    return { 
      orders: customerOrders, 
      lifetimeSpend, 
      avgSpend, 
      avgTip, 
      lastOrder,
      firstOrder,
      daysSinceLastOrder,
      mostOrderedItems, 
      totalOrders: customerOrders.length,
      channelBreakdown
    };
  };

  const handleAiQuery = async () => {
    if (!aiQuery.trim()) return;
    
    setIsAiLoading(true);
    const userMessage = { role: 'user', content: aiQuery };
    setAiMessages(prev => [...prev, userMessage]);
    setAiQuery('');

    try {
      const response = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          model: 'claude-sonnet-4-20250514',
          max_tokens: 1000,
          messages: [
            ...aiMessages,
            userMessage
          ],
          system: `You are a business intelligence assistant for a restaurant chain. You have access to the following business data:

Locations: ${JSON.stringify(mockDatabase.locations)}
Customers: ${JSON.stringify(mockDatabase.customers)}
Orders: ${JSON.stringify(mockDatabase.orders)}

Performance metrics:
- Total Revenue: $${performance.totalRevenue.toFixed(2)}
- Total Orders: ${performance.totalOrders}
- Average Order Value: $${performance.avgOrderValue.toFixed(2)}
- Total Customers: ${performance.totalCustomers}
- Loyalty Members: ${performance.loyaltyMembers}
- Average Tip: ${performance.avgTip.toFixed(1)}%

Top selling items: ${performance.topItems.map(([item, count]) => `${item} (${count})`).join(', ')}

Provide strategic insights, recommendations, and answer questions about the business. Be specific with numbers and actionable advice.`
        })
      });

      const data = await response.json();
      const assistantMessage = {
        role: 'assistant',
        content: data.content[0].text
      };
      setAiMessages(prev => [...prev, assistantMessage]);
    } catch (error) {
      console.error('AI query error:', error);
      setAiMessages(prev => [...prev, {
        role: 'assistant',
        content: 'Sorry, I encountered an error processing your question. Please try again.'
      }]);
    } finally {
      setIsAiLoading(false);
    }
  };

  const pieData = performance.topItems.map(([item, count]) => ({
    name: item,
    value: count
  }));

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="bg-white border-b border-gray-200 px-6 py-4">
        <div className="flex items-center justify-between">
          <h1 className="text-3xl font-semibold text-gray-900">Restaurant Analytics</h1>
          {activeTab === 'performance' && (
            <div className="flex items-center gap-2">
              <MapPin className="w-4 h-4 text-gray-500" />
              <select value={selectedLocation} onChange={(e) => setSelectedLocation(e.target.value)} className="px-4 py-2 border border-gray-300 rounded-lg bg-white">
                <option value="all">All Locations</option>
                {mockDatabase.locations.map(loc => (
                  <option key={loc.id} value={loc.id}>{loc.name}</option>
                ))}
              </select>
            </div>
          )}
        </div>
      </div>

      <div className="bg-white border-b border-gray-200 px-6">
        <div className="flex gap-8">
          <button onClick={() => { setActiveTab('performance'); setDrilldownData(null); }} className={`py-4 text-sm font-medium border-b-2 ${activeTab === 'performance' ? 'border-orange-500 text-gray-900' : 'border-transparent text-gray-500'}`}>
            Overall Performance
          </button>
          <button onClick={() => { setActiveTab('guests'); setSelectedCustomer(null); }} className={`py-4 text-sm font-medium border-b-2 ${activeTab === 'guests' ? 'border-orange-500 text-gray-900' : 'border-transparent text-gray-500'}`}>
            Guest Management
          </button>
          <button onClick={() => setActiveTab('ai')} className={`py-4 text-sm font-medium border-b-2 ${activeTab === 'ai' ? 'border-orange-500 text-gray-900' : 'border-transparent text-gray-500'}`}>
            AI Query
          </button>
        </div>
      </div>

      <div className="p-6">
        {activeTab === 'performance' && !drilldownData && (
          <div className="space-y-6">
            <div className="grid grid-cols-4 gap-4">
              <div onClick={() => setDrilldownData({ type: 'revenue', data: performance.recentOrders })} className="bg-white p-6 rounded-lg border border-gray-200 cursor-pointer hover:shadow-lg transition-shadow">
                <div className="flex items-center justify-between mb-2">
                  <div className="text-sm text-gray-600">Total Revenue</div>
                  <DollarSign className="w-5 h-5 text-green-500" />
                </div>
                <div className="text-3xl font-bold text-gray-900">${performance.totalRevenue.toFixed(2)}</div>
                <div className="text-xs text-gray-500 mt-1 flex items-center gap-1">Click to view <ChevronRight className="w-3 h-3" /></div>
              </div>

              <div onClick={() => setDrilldownData({ type: 'orders', data: performance.recentOrders })} className="bg-white p-6 rounded-lg border border-gray-200 cursor-pointer hover:shadow-lg transition-shadow">
                <div className="flex items-center justify-between mb-2">
                  <div className="text-sm text-gray-600">Total Orders</div>
                  <ShoppingBag className="w-5 h-5 text-blue-500" />
                </div>
                <div className="text-3xl font-bold text-gray-900">{performance.totalOrders}</div>
                <div className="text-xs text-gray-500 mt-1 flex items-center gap-1">Click to view <ChevronRight className="w-3 h-3" /></div>
              </div>

              <div className="bg-white p-6 rounded-lg border border-gray-200">
                <div className="flex items-center justify-between mb-2">
                  <div className="text-sm text-gray-600">Avg Order Value</div>
                  <TrendingUp className="w-5 h-5 text-purple-500" />
                </div>
                <div className="text-3xl font-bold text-gray-900">${performance.avgOrderValue.toFixed(2)}</div>
                <div className="text-xs text-green-600 mt-1">+5.2% vs last month</div>
              </div>

              <div onClick={() => setDrilldownData({ type: 'customers', data: performance.topCustomers })} className="bg-white p-6 rounded-lg border border-gray-200 cursor-pointer hover:shadow-lg transition-shadow">
                <div className="flex items-center justify-between mb-2">
                  <div className="text-sm text-gray-600">Total Customers</div>
                  <Users className="w-5 h-5 text-orange-500" />
                </div>
                <div className="text-3xl font-bold text-gray-900">{performance.totalCustomers}</div>
                <div className="text-xs text-gray-500 mt-1 flex items-center gap-1">Click to view <ChevronRight className="w-3 h-3" /></div>
              </div>
            </div>

            <div className="grid grid-cols-2 gap-6">
              <div className="bg-white rounded-lg border border-gray-200 p-6">
                <h3 className="text-lg font-semibold mb-4">Revenue Trend</h3>
                <ResponsiveContainer width="100%" height={300}>
                  <LineChart data={performance.revenueData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="date" />
                    <YAxis />
                    <Tooltip />
                    <Legend />
                    <Line type="monotone" dataKey="revenue" stroke="#f97316" strokeWidth={2} name="Revenue ($)" />
                  </LineChart>
                </ResponsiveContainer>
              </div>

              <div className="bg-white rounded-lg border border-gray-200 p-6">
                <h3 className="text-lg font-semibold mb-4">Location Performance</h3>
                <ResponsiveContainer width="100%" height={300}>
                  <BarChart data={performance.locationData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="name" />
                    <YAxis />
                    <Tooltip />
                    <Legend />
                    <Bar dataKey="revenue" fill="#3b82f6" name="Revenue ($)" />
                    <Bar dataKey="orders" fill="#8b5cf6" name="Orders" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>

            <div className="grid grid-cols-2 gap-6">
              <div className="bg-white rounded-lg border border-gray-200 p-6">
                <h3 className="text-lg font-semibold mb-4">Top Selling Items Distribution</h3>
                <ResponsiveContainer width="100%" height={300}>
                  <PieChart>
                    <Pie
                      data={pieData}
                      cx="50%"
                      cy="50%"
                      labelLine={false}
                      label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
                      outerRadius={100}
                      fill="#8884d8"
                      dataKey="value"
                    >
                      {pieData.map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                      ))}
                    </Pie>
                    <Tooltip />
                  </PieChart>
                </ResponsiveContainer>
              </div>

              <div onClick={() => setDrilldownData({ type: 'topCustomers', data: performance.topCustomers })} className="bg-white rounded-lg border border-gray-200 cursor-pointer hover:shadow-lg transition-shadow">
                <div className="p-6 border-b border-gray-200 flex items-center justify-between">
                  <h3 className="text-lg font-semibold">Top Customers</h3>
                  <ChevronRight className="w-5 h-5 text-gray-400" />
                </div>
                <div className="p-6 space-y-3">
                  {performance.topCustomers.map((data, idx) => (
                    <div key={idx} className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
                      <div className="flex items-center gap-3">
                        <div className="w-8 h-8 bg-blue-100 text-blue-600 rounded-full flex items-center justify-center font-semibold text-sm">{idx + 1}</div>
                        <div>
                          <div className="text-sm font-medium text-gray-700">{data.customer?.firstName} {data.customer?.lastName}</div>
                          <div className="text-xs text-gray-500">{data.orders} orders</div>
                        </div>
                      </div>
                      <span className="text-sm font-bold text-gray-900">${data.spend.toFixed(2)}</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </div>
        )}

        {activeTab === 'performance' && drilldownData && (
          <div className="space-y-4">
            <button onClick={() => setDrilldownData(null)} className="flex items-center gap-2 text-blue-600 hover:text-blue-700">
              <X className="w-4 h-4" />
              <span className="text-sm font-medium">Back to Dashboard</span>
            </button>

            <div className="bg-white rounded-lg border border-gray-200">
              <div className="px-6 py-4 border-b border-gray-200">
                <h2 className="text-lg font-semibold">
                  {drilldownData.type === 'revenue' && 'Revenue Details'}
                  {drilldownData.type === 'orders' && 'Order Details'}
                  {drilldownData.type === 'customers' && 'Top Customers'}
                  {drilldownData.type === 'items' && 'Top Items'}
                  {drilldownData.type === 'topCustomers' && 'Customer Rankings'}
                </h2>
              </div>

              {drilldownData.type === 'items' ? (
                <div className="p-6 space-y-4">
                  {drilldownData.data.map(([item, count], idx) => (
                    <div key={item} className="flex items-center justify-between p-4 border border-gray-200 rounded-lg">
                      <div className="flex items-center gap-4">
                        <div className="w-12 h-12 bg-orange-100 text-orange-600 rounded-full flex items-center justify-center font-bold">{idx + 1}</div>
                        <div>
                          <div className="font-semibold">{item}</div>
                          <div className="text-sm text-gray-500">Ordered {count} times</div>
                        </div>
                      </div>
                      <div className="text-2xl font-bold">{count}</div>
                    </div>
                  ))}
                </div>
              ) : (
                <table className="w-full">
                  <thead className="bg-gray-50">
                    <tr>
                      {drilldownData.type === 'orders' || drilldownData.type === 'revenue' ? (
                        <>
                          <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Order</th>
                          <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Customer</th>
                          <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Amount</th>
                          <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Server</th>
                          <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Date</th>
                        </>
                      ) : (
                        <>
                          <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Rank</th>
                          <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Customer</th>
                          <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Email</th>
                          <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Orders</th>
                          <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Total</th>
                        </>
                      )}
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-200">
                    {drilldownData.type === 'orders' || drilldownData.type === 'revenue' ? (
                      drilldownData.data.map(order => {
                        const customer = mockDatabase.customers.find(c => c.id === order.customerId);
                        return (
                          <tr key={order.id} className="hover:bg-gray-50">
                            <td className="px-6 py-4 text-sm">{order.id}</td>
                            <td className="px-6 py-4 text-sm">{customer ? `${customer.firstName} ${customer.lastName}` : 'N/A'}</td>
                            <td className="px-6 py-4 text-sm font-semibold">${order.amount.toFixed(2)}</td>
                            <td className="px-6 py-4 text-sm">{order.server}</td>
                            <td className="px-6 py-4 text-sm">{new Date(order.date).toLocaleDateString()}</td>
                          </tr>
                        );
                      })
                    ) : (
                      drilldownData.data.map((data, idx) => (
                        <tr key={idx} className="hover:bg-gray-50">
                          <td className="px-6 py-4 text-sm">#{idx + 1}</td>
                          <td className="px-6 py-4 text-sm font-medium">{data.customer?.firstName} {data.customer?.lastName}</td>
                          <td className="px-6 py-4 text-sm">{data.customer?.email}</td>
                          <td className="px-6 py-4 text-sm">{data.orders}</td>
                          <td className="px-6 py-4 text-sm font-semibold">${data.spend.toFixed(2)}</td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              )}
            </div>
          </div>
        )}

        {activeTab === 'guests' && !selectedCustomer && (
          <div className="space-y-4">
            <div className="bg-white p-4 rounded-lg border border-gray-200">
              <div className="relative">
                <Search className="absolute left-3 top-3 w-5 h-5 text-gray-400" />
                <input type="text" value={searchQuery} onChange={(e) => setSearchQuery(e.target.value)} placeholder="Search guests..." className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg" />
              </div>
            </div>

            <div className="bg-white rounded-lg border border-gray-200">
              <div className="px-6 py-4 border-b border-gray-200">
                <h2 className="text-lg font-semibold">All Guests ({filteredCustomers.length})</h2>
              </div>
              <div className="divide-y divide-gray-200">
                {filteredCustomers.map(customer => {
                  const metrics = getCustomerMetrics(customer.id);
                  return (
                    <div key={customer.id} onClick={() => setSelectedCustomer(customer)} className="p-6 hover:bg-gray-50 cursor-pointer">
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-4">
                          <div className="w-12 h-12 bg-gradient-to-br from-blue-500 to-purple-500 rounded-full flex items-center justify-center text-white font-semibold">{customer.firstName[0]}{customer.lastName[0]}</div>
                          <div>
                            <div className="flex items-center gap-2">
                              <h3 className="text-lg font-semibold">{customer.firstName} {customer.lastName}</h3>
                              {customer.loyaltyStatus === 'active' && <span className="px-2 py-1 bg-pink-100 text-pink-700 text-xs font-medium rounded">LOYALTY</span>}
                            </div>
                            <div className="flex items-center gap-4 mt-1 text-sm text-gray-600">
                              <span>{customer.email}</span>
                              <span>{customer.phone}</span>
                            </div>
                          </div>
                        </div>
                        <div className="grid grid-cols-3 gap-6 text-right">
                          <div>
                            <div className="text-xs text-gray-500">Orders</div>
                            <div className="text-lg font-semibold">{metrics.totalOrders}</div>
                          </div>
                          <div>
                            <div className="text-xs text-gray-500">Lifetime</div>
                            <div className="text-lg font-semibold">${metrics.lifetimeSpend.toFixed(2)}</div>
                          </div>
                          <div>
                            <div className="text-xs text-gray-500">Points</div>
                            <div className="text-lg font-semibold">{customer.loyaltyPoints}</div>
                          </div>
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          </div>
        )}

        {activeTab === 'guests' && selectedCustomer && (
          <div className="space-y-6">
            <button onClick={() => setSelectedCustomer(null)} className="flex items-center gap-2 text-blue-600 hover:text-blue-700">
              <X className="w-4 h-4" />
              <span className="text-sm font-medium">Back to guests</span>
            </button>

            <div className="bg-white p-6 rounded-lg border border-gray-200">
              <div className="flex items-center justify-between mb-6">
                <div className="flex items-center gap-4">
                  <div className="w-16 h-16 bg-gradient-to-br from-blue-500 to-purple-500 rounded-full flex items-center justify-center text-white font-semibold text-2xl">{selectedCustomer.firstName[0]}{selectedCustomer.lastName[0]}</div>
                  <div>
                    <h2 className="text-2xl font-semibold">{selectedCustomer.firstName} {selectedCustomer.lastName}</h2>
                    <div className="flex items-center gap-4 mt-1 text-sm text-gray-600">
                      <span>{selectedCustomer.phone}</span>
                      <span>{selectedCustomer.email}</span>
                    </div>
                  </div>
                </div>
                {selectedCustomer.loyaltyStatus === 'active' && (
                  <span className="px-3 py-1 bg-pink-100 text-pink-700 text-sm font-medium rounded">LOYALTY</span>
                )}
              </div>

              {/* Tabs Navigation */}
              <div className="border-b border-gray-200 mb-6">
                <div className="flex gap-8">
                  {['Overview', 'Orders', 'Loyalty', 'Marketing', 'Details'].map(tab => (
                    <button
                      key={tab}
                      onClick={() => setSelectedCustomer({...selectedCustomer, activeSubTab: tab.toLowerCase()})}
                      className={`py-3 text-sm font-medium border-b-2 ${
                        (selectedCustomer.activeSubTab || 'overview') === tab.toLowerCase()
                          ? 'border-orange-500 text-gray-900'
                          : 'border-transparent text-gray-500'
                      }`}
                    >
                      {tab} {tab === 'Orders' && `${getCustomerMetrics(selectedCustomer.id).totalOrders}`}
                    </button>
                  ))}
                </div>
              </div>

              {/* Overview Tab */}
              {(!selectedCustomer.activeSubTab || selectedCustomer.activeSubTab === 'overview') && (
                <>
                  <div className="grid grid-cols-4 gap-4 mb-6">
                    {(() => {
                      const metrics = getCustomerMetrics(selectedCustomer.id);
                      return (
                        <>
                          <div className="p-4 bg-gray-50 rounded-lg">
                            <div className="text-sm text-gray-600 mb-1">Last order</div>
                            <div className="text-xl font-semibold">{metrics.daysSinceLastOrder} days ago</div>
                          </div>
                          <div className="p-4 bg-gray-50 rounded-lg">
                            <div className="text-sm text-gray-600 mb-1">Lifetime spend</div>
                            <div className="text-xl font-semibold">${metrics.lifetimeSpend.toFixed(2)}</div>
                          </div>
                          <div className="p-4 bg-gray-50 rounded-lg">
                            <div className="text-sm text-gray-600 mb-1">Average spend</div>
                            <div className="text-xl font-semibold">${metrics.avgSpend.toFixed(2)}</div>
                          </div>
                          <div className="p-4 bg-gray-50 rounded-lg">
                            <div className="text-sm text-gray-600 mb-1">Average tip</div>
                            <div className="text-xl font-semibold">{metrics.avgTip.toFixed(0)}%</div>
                          </div>
                        </>
                      );
                    })()}
                  </div>

                  <div className="grid grid-cols-2 gap-6 mb-6">
                    {(() => {
                      const metrics = getCustomerMetrics(selectedCustomer.id);
                      return (
                        <>
                          <div className="border border-gray-200 rounded-lg p-6">
                            <h3 className="font-semibold mb-4">Order channels</h3>
                            <div className="space-y-3">
                              {metrics.channelBreakdown.map(channel => (
                                <div key={channel.type} className="flex items-center justify-between">
                                  <div className="flex items-center gap-2">
                                    <div className={`w-3 h-3 rounded-full ${channel.type === 'Dine-in' ? 'bg-orange-500' : channel.type === 'Pickup' ? 'bg-blue-500' : 'bg-purple-500'}`}></div>
                                    <span className="text-sm">{channel.type}</span>
                                  </div>
                                  <span className="text-sm font-semibold">{channel.percentage}%</span>
                                </div>
                              ))}
                            </div>
                            <div className="mt-4 flex justify-center">
                              <div className="text-center">
                                <div className="w-24 h-24 rounded-full border-8 border-orange-500 flex items-center justify-center">
                                  <span className="text-2xl font-bold">{metrics.totalOrders}</span>
                                </div>
                              </div>
                            </div>
                          </div>

                          <div className="border border-gray-200 rounded-lg p-6">
                            <h3 className="font-semibold mb-4">Most ordered items</h3>
                            <div className="space-y-3">
                              {metrics.mostOrderedItems.map(([item, count]) => (
                                <div key={item} className="flex items-center justify-between py-2">
                                  <span className="text-sm">{item}</span>
                                  <span className="text-sm font-semibold">{count}x</span>
                                </div>
                              ))}
                            </div>
                          </div>
                        </>
                      );
                    })()}
                  </div>

                  <div className="border border-gray-200 rounded-lg">
                    <div className="px-6 py-4 border-b border-gray-200">
                      <h3 className="font-semibold">Activity</h3>
                    </div>
                    <div className="divide-y divide-gray-200">
                      {getCustomerMetrics(selectedCustomer.id).orders.slice(0, 4).map(order => (
                        <div key={order.id} className="px-6 py-4 flex items-center justify-between hover:bg-gray-50">
                          <div className="flex items-center gap-4">
                            <div className="w-10 h-10 bg-blue-100 rounded-full flex items-center justify-center">
                              <ShoppingBag className="w-5 h-5 text-blue-600" />
                            </div>
                            <div>
                              <div className="font-medium text-sm">Order for ${order.amount.toFixed(2)}</div>
                              <div className="text-xs text-gray-500">{new Date(order.date).toLocaleDateString()}</div>
                            </div>
                          </div>
                          <div className="text-right">
                            <div className="text-sm">{new Date(order.date).toLocaleTimeString([], {hour: '2-digit', minute: '2-digit'})}</div>
                            <div className="text-xs text-gray-500">{new Date(order.date).toLocaleDateString('en-US', {month: 'short', day: 'numeric', year: 'numeric'})}</div>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                </>
              )}

              {/* Orders Tab */}
              {selectedCustomer.activeSubTab === 'orders' && (
                <div>
                  <div className="grid grid-cols-3 gap-4 mb-6">
                    {(() => {
                      const metrics = getCustomerMetrics(selectedCustomer.id);
                      return (
                        <>
                          <div className="p-4 bg-gray-50 rounded-lg">
                            <div className="text-sm text-gray-600 mb-1">Total orders</div>
                            <div className="text-2xl font-semibold">{metrics.totalOrders}</div>
                          </div>
                          <div className="p-4 bg-gray-50 rounded-lg">
                            <div className="text-sm text-gray-600 mb-1">Last order</div>
                            <div className="text-2xl font-semibold">{metrics.daysSinceLastOrder} days ago</div>
                          </div>
                          <div className="p-4 bg-gray-50 rounded-lg">
                            <div className="text-sm text-gray-600 mb-1">First order</div>
                            <div className="text-2xl font-semibold">
                              {Math.floor((new Date() - new Date(metrics.firstOrder)) / (1000 * 60 * 60 * 24 * 30))} months ago
                            </div>
                          </div>
                        </>
                      );
                    })()}
                  </div>

                  <div className="border border-gray-200 rounded-lg">
                    <div className="px-6 py-4 border-b border-gray-200">
                      <h3 className="font-semibold">Order activity</h3>
                    </div>
                    <table className="w-full">
                      <thead className="bg-gray-50">
                        <tr>
                          <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Check</th>
                          <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Order type</th>
                          <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Location</th>
                          <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Server</th>
                          <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Amount</th>
                          <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Tip</th>
                          <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Time</th>
                          <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Date</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-gray-200">
                        {getCustomerMetrics(selectedCustomer.id).orders.map(order => {
                          const location = mockDatabase.locations.find(l => l.id === order.location);
                          return (
                            <tr key={order.id} className="hover:bg-gray-50">
                              <td className="px-6 py-4 text-sm">{order.id}</td>
                              <td className="px-6 py-4 text-sm">{order.type}</td>
                              <td className="px-6 py-4 text-sm">{location?.address}</td>
                              <td className="px-6 py-4 text-sm">{order.server}</td>
                              <td className="px-6 py-4 text-sm font-semibold">${order.amount.toFixed(2)}</td>
                              <td className="px-6 py-4 text-sm">{order.tipPercent}%</td>
                              <td className="px-6 py-4 text-sm">{new Date(order.date).toLocaleTimeString([], {hour: '2-digit', minute: '2-digit'})}</td>
                              <td className="px-6 py-4 text-sm">{new Date(order.date).toLocaleDateString('en-US', {month: 'short', day: 'numeric', year: 'numeric'})}</td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {/* Loyalty Tab */}
              {selectedCustomer.activeSubTab === 'loyalty' && (
                <div>
                  <div className="grid grid-cols-3 gap-4 mb-6">
                    <div className="p-4 bg-gray-50 rounded-lg">
                      <div className="text-sm text-gray-600 mb-1">Loyalty status</div>
                      <div className="text-2xl font-semibold capitalize">{selectedCustomer.loyaltyStatus} member</div>
                    </div>
                    <div className="p-4 bg-gray-50 rounded-lg">
                      <div className="text-sm text-gray-600 mb-1">Loyalty balance</div>
                      <div className="text-2xl font-semibold">{selectedCustomer.loyaltyPoints} points</div>
                    </div>
                    <div className="p-4 bg-gray-50 rounded-lg">
                      <div className="text-sm text-gray-600 mb-1">Reward</div>
                      <div className="flex items-center gap-2">
                        <span className="text-2xl font-semibold">free food</span>
                        <span className="px-2 py-1 bg-pink-100 text-pink-700 text-xs font-medium rounded">100 POINTS</span>
                      </div>
                    </div>
                  </div>

                  <div className="border border-gray-200 rounded-lg">
                    <div className="px-6 py-4 border-b border-gray-200">
                      <h3 className="font-semibold">Loyalty activity</h3>
                      <div className="text-sm text-gray-600 mt-1">Member number {selectedCustomer.memberNumber}</div>
                    </div>
                    <table className="w-full">
                      <thead className="bg-gray-50">
                        <tr>
                          <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Activity</th>
                          <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Location</th>
                          <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Amount</th>
                          <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Balance</th>
                          <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Time</th>
                          <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Date</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-gray-200">
                        {getCustomerMetrics(selectedCustomer.id).orders.map((order, idx) => {
                          const location = mockDatabase.locations.find(l => l.id === order.location);
                          const pointsEarned = Math.floor(order.amount);
                          const runningBalance = selectedCustomer.loyaltyPoints - getCustomerMetrics(selectedCustomer.id).orders.slice(0, idx).reduce((sum, o) => sum + Math.floor(o.amount), 0);
                          
                          return (
                            <tr key={order.id} className="hover:bg-gray-50">
                              <td className="px-6 py-4 text-sm">Add Value</td>
                              <td className="px-6 py-4 text-sm">{location?.name}</td>
                              <td className="px-6 py-4 text-sm">{pointsEarned} points</td>
                              <td className="px-6 py-4 text-sm">{runningBalance} points</td>
                              <td className="px-6 py-4 text-sm">{new Date(order.date).toLocaleTimeString([], {hour: '2-digit', minute: '2-digit'})}</td>
                              <td className="px-6 py-4 text-sm">{new Date(order.date).toLocaleDateString('en-US', {month: 'short', day: 'numeric', year: 'numeric'})}</td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {/* Marketing Tab */}
              {selectedCustomer.activeSubTab === 'marketing' && (
                <div>
                  <div className="grid grid-cols-4 gap-4 mb-6">
                    <div className="p-4 bg-gray-50 rounded-lg">
                      <div className="text-sm text-gray-600 mb-1">Top channel</div>
                      <div className="text-2xl font-semibold">Email</div>
                    </div>
                    <div className="p-4 bg-gray-50 rounded-lg">
                      <div className="text-sm text-gray-600 mb-1">Open rate</div>
                      <div className="text-2xl font-semibold">100%</div>
                    </div>
                    <div className="p-4 bg-gray-50 rounded-lg">
                      <div className="text-sm text-gray-600 mb-1">Attributed spend</div>
                      <div className="text-2xl font-semibold">$287</div>
                    </div>
                    <div className="p-4 bg-gray-50 rounded-lg">
                      <div className="text-sm text-gray-600 mb-1">Time to purchase</div>
                      <div className="text-2xl font-semibold">5.33 days</div>
                    </div>
                  </div>

                  <div className="grid grid-cols-3 gap-6 mb-6">
                    <div className="border border-gray-200 rounded-lg p-6">
                      <h3 className="font-semibold mb-4">Subscription status</h3>
                      <div className="space-y-2">
                        <div className="flex items-center justify-between">
                          <span className="text-sm">Email</span>
                          <span className="px-2 py-1 bg-green-100 text-green-700 text-xs font-medium rounded">SUBSCRIBED</span>
                        </div>
                        <div className="flex items-center justify-between">
                          <span className="text-sm">SMS</span>
                          <span className="px-2 py-1 bg-yellow-100 text-yellow-700 text-xs font-medium rounded">NOT SUBSCRIBED</span>
                        </div>
                      </div>
                    </div>

                    <div className="border border-gray-200 rounded-lg p-6">
                      <h3 className="font-semibold mb-4">Send frequency by channel</h3>
                      <div className="space-y-2">
                        <div className="flex items-center justify-between">
                          <span className="text-sm">Email</span>
                          <span className="text-sm text-gray-600">Monthly</span>
                        </div>
                        <div className="flex items-center justify-between">
                          <span className="text-sm">SMS</span>
                          <span className="text-sm text-gray-600">Never</span>
                        </div>
                      </div>
                    </div>

                    <div className="border border-gray-200 rounded-lg p-6">
                      <h3 className="font-semibold mb-4">Open rate by channel</h3>
                      <div className="space-y-2">
                        <div className="flex items-center justify-between">
                          <span className="text-sm">Email</span>
                          <span className="text-sm font-semibold">100%</span>
                        </div>
                        <div className="flex items-center justify-between">
                          <span className="text-sm">SMS</span>
                          <span className="text-sm font-semibold">0%</span>
                        </div>
                      </div>
                    </div>
                  </div>

                  <div className="border border-gray-200 rounded-lg">
                    <div className="px-6 py-4 border-b border-gray-200">
                      <h3 className="font-semibold">Campaign activity</h3>
                    </div>
                    <table className="w-full">
                      <thead className="bg-gray-50">
                        <tr>
                          <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Campaign</th>
                          <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Channel</th>
                          <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Date</th>
                          <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Status</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-gray-200">
                        <tr className="hover:bg-gray-50">
                          <td className="px-6 py-4 text-sm">Pumpkin Xiao Long Bao - WA01-15</td>
                          <td className="px-6 py-4 text-sm">Email</td>
                          <td className="px-6 py-4 text-sm">Sep 22, 2025</td>
                          <td className="px-6 py-4"><span className="px-2 py-1 bg-green-100 text-green-700 text-xs font-medium rounded">OPENED</span></td>
                        </tr>
                        <tr className="hover:bg-gray-50">
                          <td className="px-6 py-4 text-sm">Q-BAO Daily journey - CA & WA01</td>
                          <td className="px-6 py-4 text-sm">Email</td>
                          <td className="px-6 py-4 text-sm">Aug 15, 2025</td>
                          <td className="px-6 py-4"><span className="px-2 py-1 bg-green-100 text-green-700 text-xs font-medium rounded">OPENED</span></td>
                        </tr>
                        <tr className="hover:bg-gray-50">
                          <td className="px-6 py-4 text-sm">Q-BAO Daily journey - WA(exclude WA01)</td>
                          <td className="px-6 py-4 text-sm">Email</td>
                          <td className="px-6 py-4 text-sm">Aug 15, 2025</td>
                          <td className="px-6 py-4"><span className="px-2 py-1 bg-green-100 text-green-700 text-xs font-medium rounded">OPENED</span></td>
                        </tr>
                        <tr className="hover:bg-gray-50">
                          <td className="px-6 py-4 text-sm">Bite of Seattle live cook show - WA(exclude WA01)</td>
                          <td className="px-6 py-4 text-sm">Email</td>
                          <td className="px-6 py-4 text-sm">Jul 24, 2025</td>
                          <td className="px-6 py-4"><span className="px-2 py-1 bg-green-100 text-green-700 text-xs font-medium rounded">OPENED</span></td>
                        </tr>
                        <tr className="hover:bg-gray-50">
                          <td className="px-6 py-4 text-sm">Bite of Seattle live cook show - WA01</td>
                          <td className="px-6 py-4 text-sm">Email</td>
                          <td className="px-6 py-4 text-sm">Jul 24, 2025</td>
                          <td className="px-6 py-4"><span className="px-2 py-1 bg-green-100 text-green-700 text-xs font-medium rounded">OPENED</span></td>
                        </tr>
                        <tr className="hover:bg-gray-50">
                          <td className="px-6 py-4 text-sm">Summer Drinks/Rainbow Jelly Promotion</td>
                          <td className="px-6 py-4 text-sm">Email</td>
                          <td className="px-6 py-4 text-sm">Jun 17, 2025</td>
                          <td className="px-6 py-4"><span className="px-2 py-1 bg-green-100 text-green-700 text-xs font-medium rounded">OPENED</span></td>
                        </tr>
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {/* Details Tab */}
              {selectedCustomer.activeSubTab === 'details' && (
                <div className="space-y-6">
                  <div className="border border-gray-200 rounded-lg p-6">
                    <h3 className="font-semibold mb-4">Name</h3>
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <label className="block text-sm text-gray-600 mb-2">First name</label>
                        <input type="text" value={selectedCustomer.firstName} readOnly className="w-full px-4 py-2 border border-gray-300 rounded-lg bg-gray-50" />
                      </div>
                      <div>
                        <label className="block text-sm text-gray-600 mb-2">Last name</label>
                        <input type="text" value={selectedCustomer.lastName} readOnly className="w-full px-4 py-2 border border-gray-300 rounded-lg bg-gray-50" />
                      </div>
                    </div>
                  </div>

                  <div className="border border-gray-200 rounded-lg p-6">
                    <h3 className="font-semibold mb-4">Contact</h3>
                    <div className="space-y-4">
                      <div>
                        <label className="block text-sm text-gray-600 mb-2">Phone number</label>
                        <div className="flex items-center justify-between px-4 py-2 border border-gray-300 rounded-lg bg-gray-50">
                          <span>{selectedCustomer.phone}</span>
                          <span className="px-2 py-1 bg-yellow-100 text-yellow-700 text-xs font-medium rounded">NOT SUBSCRIBED</span>
                        </div>
                      </div>
                      <div>
                        <label className="block text-sm text-gray-600 mb-2">Email address</label>
                        <div className="flex items-center justify-between px-4 py-2 border border-gray-300 rounded-lg bg-gray-50">
                          <span>{selectedCustomer.email}</span>
                          <span className="px-2 py-1 bg-green-100 text-green-700 text-xs font-medium rounded">SUBSCRIBED</span>
                        </div>
                      </div>
                    </div>
                  </div>

                  <div className="grid grid-cols-2 gap-6">
                    <div className="border border-gray-200 rounded-lg p-6">
                      <h3 className="font-semibold mb-4">Tags</h3>
                      <div className="text-sm text-gray-600 mb-3">{mockDatabase.locations.find(l => l.id === selectedCustomer.location)?.name}</div>
                      <div className="space-y-2">
                        {['VIP', 'Friend of Owner', 'Friend of Staff', 'Influencer', 'Local', 'Industry', 'Investor'].map(tag => (
                          <label key={tag} className="flex items-center gap-2 text-sm">
                            <input type="checkbox" className="w-4 h-4 text-orange-500 border-gray-300 rounded" />
                            <span>{tag}</span>
                          </label>
                        ))}
                      </div>
                    </div>

                    <div className="border border-gray-200 rounded-lg p-6">
                      <h3 className="font-semibold mb-4">Notes</h3>
                      <div className="text-sm text-gray-600 mb-3">{mockDatabase.locations.find(l => l.id === selectedCustomer.location)?.name}</div>
                      <textarea 
                        placeholder="Add a note"
                        className="w-full px-4 py-2 border border-gray-300 rounded-lg resize-none"
                        rows="6"
                      ></textarea>
                    </div>
                  </div>
                </div>
              )}
            </div>
          </div>
        )}


        {activeTab === 'ai' && (
          <div className="max-w-4xl mx-auto">
            <div className="bg-white rounded-lg border border-gray-200 shadow-sm">
              <div className="px-6 py-4 border-b border-gray-200 bg-gradient-to-r from-orange-50 to-purple-50">
                <div className="flex items-center gap-3">
                  <MessageSquare className="w-6 h-6 text-orange-600" />
                  <div>
                    <h2 className="text-lg font-semibold text-gray-900">AI Business Intelligence</h2>
                    <p className="text-sm text-gray-600">Ask strategic questions about your business data</p>
                  </div>
                </div>
              </div>

              <div className="h-96 overflow-y-auto p-6 space-y-4">
                {aiMessages.length === 0 ? (
                  <div className="text-center py-12">
                    <MessageSquare className="w-12 h-12 text-gray-300 mx-auto mb-4" />
                    <h3 className="text-lg font-medium text-gray-900 mb-2">Start a conversation</h3>
                    <p className="text-sm text-gray-500 mb-6">Ask questions about your business performance, customer insights, or strategic recommendations</p>
                    <div className="grid grid-cols-2 gap-3 max-w-2xl mx-auto">
                      <button onClick={() => setAiQuery('What are our best performing locations and why?')} className="p-3 text-left bg-gray-50 hover:bg-gray-100 rounded-lg border border-gray-200 text-sm">
                        <div className="font-medium text-gray-900">Location Performance</div>
                        <div className="text-gray-500 text-xs mt-1">Analyze best performing locations</div>
                      </button>
                      <button onClick={() => setAiQuery('How can we increase customer loyalty?')} className="p-3 text-left bg-gray-50 hover:bg-gray-100 rounded-lg border border-gray-200 text-sm">
                        <div className="font-medium text-gray-900">Loyalty Strategy</div>
                        <div className="text-gray-500 text-xs mt-1">Get recommendations for retention</div>
                      </button>
                      <button onClick={() => setAiQuery('Which menu items should we promote more?')} className="p-3 text-left bg-gray-50 hover:bg-gray-100 rounded-lg border border-gray-200 text-sm">
                        <div className="font-medium text-gray-900">Menu Optimization</div>
                        <div className="text-gray-500 text-xs mt-1">Identify promotion opportunities</div>
                      </button>
                      <button onClick={() => setAiQuery('What trends do you see in our customer data?')} className="p-3 text-left bg-gray-50 hover:bg-gray-100 rounded-lg border border-gray-200 text-sm">
                        <div className="font-medium text-gray-900">Customer Trends</div>
                        <div className="text-gray-500 text-xs mt-1">Discover customer patterns</div>
                      </button>
                    </div>
                  </div>
                ) : (
                  aiMessages.map((msg, idx) => (
                    <div key={idx} className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}>
                      <div className={`max-w-2xl rounded-lg px-4 py-3 ${msg.role === 'user' ? 'bg-orange-500 text-white' : 'bg-gray-100 text-gray-900'}`}>
                        <div className="text-sm whitespace-pre-wrap">{msg.content}</div>
                      </div>
                    </div>
                  ))
                )}
                {isAiLoading && (
                  <div className="flex justify-start">
                    <div className="max-w-2xl rounded-lg px-4 py-3 bg-gray-100">
                      <div className="flex items-center gap-2">
                        <div className="w-2 h-2 bg-gray-400 rounded-full animate-bounce"></div>
                        <div className="w-2 h-2 bg-gray-400 rounded-full animate-bounce" style={{ animationDelay: '0.2s' }}></div>
                        <div className="w-2 h-2 bg-gray-400 rounded-full animate-bounce" style={{ animationDelay: '0.4s' }}></div>
                      </div>
                    </div>
                  </div>
                )}
              </div>

              <div className="px-6 py-4 border-t border-gray-200 bg-gray-50">
                <div className="flex gap-2">
                  <input
                    type="text"
                    value={aiQuery}
                    onChange={(e) => setAiQuery(e.target.value)}
                    onKeyPress={(e) => e.key === 'Enter' && handleAiQuery()}
                    placeholder="Ask a business question..."
                    className="flex-1 px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-orange-500"
                    disabled={isAiLoading}
                  />
                  <button
                    onClick={handleAiQuery}
                    disabled={isAiLoading || !aiQuery.trim()}
                    className="px-6 py-2 bg-orange-500 text-white rounded-lg hover:bg-orange-600 disabled:bg-gray-300 disabled:cursor-not-allowed flex items-center gap-2"
                  >
                    <Send className="w-4 h-4" />
                    Send
                  </button>
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default DashboardApp;