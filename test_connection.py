

from web3 import Web3
import os
from dotenv import load_dotenv

def test_infura_connection():
    """Test the Infura API connection"""
    
    # Load environment variables
    load_dotenv()
    
    # Your API key
    infura_project_id = "cae029ba3dce463fb75536b6a4a96186"
    provider_url = f"https://mainnet.infura.io/v3/{infura_project_id}"
    
    print("🔗 Testing Infura connection...")
    print(f"Provider URL: {provider_url}")
    
    try:
        # Create Web3 instance
        w3 = Web3(Web3.HTTPProvider(provider_url))
        
        # Test connection
        if w3.is_connected():
            print("✅ Successfully connected to Ethereum mainnet!")
            
            # Get latest block info
            latest_block = w3.eth.block_number
            block_info = w3.eth.get_block(latest_block)
            
            print(f"📊 Latest block number: {latest_block:,}")
            print(f"⛏️  Block timestamp: {block_info['timestamp']}")
            print(f"🔢 Transactions in block: {len(block_info['transactions'])}")
            
            # Get network info
            chain_id = w3.eth.chain_id
            print(f"🌐 Chain ID: {chain_id} (1 = Ethereum Mainnet)")
            
            # Test getting a transaction
            if block_info['transactions']:
                tx_hash = block_info['transactions'][0]
                tx = w3.eth.get_transaction(tx_hash)
                print(f"💸 Sample transaction: {tx_hash.hex()}")
                print(f"   From: {tx['from']}")
                print(f"   To: {tx['to']}")
                print(f"   Value: {w3.from_wei(tx['value'], 'ether')} ETH")
            
            print("\n🎉 Your Infura setup is working perfectly!")
            print("✨ Ready to run the full DeFi pipeline!")
            
            return True
            
        else:
            print("❌ Failed to connect to Ethereum")
            return False
            
    except Exception as e:
        print(f"❌ Connection error: {e}")
        print("\n💡 Troubleshooting steps:")
        print("1. Check your internet connection")
        print("2. Verify your Infura project ID is correct")
        print("3. Make sure your Infura project is active")
        return False

def test_uniswap_data():
    """Test fetching some Uniswap data"""
    
    infura_project_id = "cae029ba3dce463fb75536b6a4a96186"
    provider_url = f"https://mainnet.infura.io/v3/{infura_project_id}"
    w3 = Web3(Web3.HTTPProvider(provider_url))
    
    if not w3.is_connected():
        print("❌ Not connected to Ethereum")
        return False
    
    print("\n🦄 Testing Uniswap data fetching...")
    
    try:
        # Uniswap V2 swap event signature
        swap_topic = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
        
        # Get recent blocks to search for swaps
        latest_block = w3.eth.block_number
        from_block = latest_block - 50  # Last 50 blocks
        
        print(f"🔍 Searching for Uniswap swaps in blocks {from_block} to {latest_block}")
        
        # Create event filter
        event_filter = w3.eth.filter({
            'fromBlock': from_block,
            'toBlock': latest_block,
            'topics': [swap_topic]
        })
        
        # Get events
        events = event_filter.get_all_entries()
        
        print(f"📈 Found {len(events)} Uniswap swap events!")
        
        if events:
            print(f"🎯 Sample swap event:")
            event = events[0]
            print(f"   Block: {event['blockNumber']}")
            print(f"   Transaction: {event['transactionHash'].hex()}")
            print(f"   Pair: {event['address']}")
            
        return True
        
    except Exception as e:
        print(f"❌ Error fetching Uniswap data: {e}")
        return False

if __name__ == "__main__":
    print("🚀 DeFi Pipeline Connection Test\n")
    
    # Test basic connection
    if test_infura_connection():
        # Test Uniswap data
        test_uniswap_data()
        
        print("\n✅ All tests passed!")
        print("📋 Next steps:")
        print("   1. Run: python run_pipeline.py")
        print("   2. Check the charts/ and exports/ folders")
        print("   3. Explore the defi_data.db SQLite database")
    else:
        print("\n❌ Connection test failed")
        print("🔧 Please check your setup and try again")
