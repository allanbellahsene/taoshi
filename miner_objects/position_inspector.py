from collections import defaultdict

import bittensor as bt
import time

from miner_config import MinerConfig
from template.protocol import GetPositions


class PositionInspector:
    MAX_RETRIES = 1
    INITIAL_RETRY_DELAY = 3  # seconds
    UPDATE_INTERVAL_S = 5 * 60  # 5 minutes

    def __init__(self, wallet, metagraph, config):
        self.wallet = wallet
        self.metagraph = metagraph
        self.config = config
        self.last_update_time = 0
        self.recently_acked_validators = []
        self.stop_requested = False  # Flag to control the loop
        self.is_testnet = self.config.subtensor.network == "test"

    def run_update_loop(self):
        while not self.stop_requested:
            try:
                self.log_validator_positions()
            except Exception as e:
                # Handle exceptions or log errors
                bt.logging.error(f"Error during position inspector update: {e}. Please alert a team member ASAP!")
            time.sleep(1)  # Don't busy loop

    def stop_update_loop(self):
        self.stop_requested = True

    def get_recently_acked_validators(self):
        return self.recently_acked_validators

    def get_possible_validators(self):
        # Right now bittensor has no functionality to know if a hotkey 100% corresponds to a validator
        # Revisit this in the future.
        if self.is_testnet:
            return self.metagraph.axons
        else:
            return [n.axon_info for n in self.metagraph.neurons
                    if n.stake > bt.Balance(MinerConfig.STAKE_MIN)
                    and n.axon_info.ip != MinerConfig.AXON_NO_IP]

    def query_positions(self, validators, hotkey_to_positions):
        remaining_validators_to_query = [v for v in validators if v.hotkey not in hotkey_to_positions]
        responses = bt.dendrite(wallet=self.wallet).query(remaining_validators_to_query, GetPositions(), deserialize=True)
        hotkey_to_v_trust = {neuron.hotkey: neuron.validator_trust for neuron in self.metagraph.neurons}
        ret = []
        for validator, response in zip(remaining_validators_to_query, responses):
            v_trust = hotkey_to_v_trust.get(validator.hotkey, 0)
            if response.error_message and v_trust >= MinerConfig.HIGH_V_TRUST_THRESHOLD:
                bt.logging.warning(f"Error getting positions from {validator}. v_trust {v_trust} Error message: {response.error_message}")
            if response.successfully_processed:
                ret.append((validator, response.positions))

        return ret

    def reconcile_validator_positions(self, hotkey_to_positions, validators):
        hotkey_to_validator = {v.hotkey: v for v in validators}
        hotkey_to_v_trust = {neuron.hotkey: neuron.validator_trust for neuron in self.metagraph.neurons}
        orders_count = defaultdict(int)
        max_order_count = 0
        corresponding_positions = []
        corresponding_hotkey = None
        for hotkey, positions in hotkey_to_positions.items():
            hotkey_total_portfolio_leverage = 0
            for position in positions:
                orders_count[hotkey] += len(position['orders'])
                hotkey_total_portfolio_leverage += abs(position['net_leverage'])

            if hotkey_total_portfolio_leverage >= 10:
                bt.logging.warning(
                    f"Validator {hotkey} has a total portfolio leverage of {hotkey_total_portfolio_leverage}. "
                    f"High leverage on crypto trade pairs comes with high fees which greatly increase your draw down.")

            if orders_count[hotkey] > max_order_count:
                max_order_count = orders_count[hotkey]
                corresponding_positions = positions
                corresponding_hotkey = hotkey


        unique_counts = set(orders_count.values())

        if len(unique_counts) > 1:
            bt.logging.warning("Spilling hotkey to positions:")
            for hotkey, count in orders_count.items():
                axon_info = hotkey_to_validator[hotkey]
                bt.logging.warning(f"Validator {hotkey} has {count} orders with v_trust {hotkey_to_v_trust[hotkey]}, axon: {axon_info}. "
                                   f"Validators may be mis-synced.")
                for i, position in enumerate(hotkey_to_positions[hotkey]):
                    bt.logging.warning(f"Position {i}: {position}")


        # Return the position in hotkey_to_positions that has the most orders
        bt.logging.info(f"Validator with the most orders: {corresponding_hotkey}, n_orders: {max_order_count}, v_trust:"
                        f" {hotkey_to_v_trust.get(corresponding_hotkey, 0)}. Corresponding positions: {corresponding_positions}")
        return corresponding_positions

    def get_positions_with_retry(self, validators_to_query):
        attempts = 0
        delay = self.INITIAL_RETRY_DELAY
        hotkey_to_positions = {}
        while attempts < self.MAX_RETRIES and len(hotkey_to_positions) != len(validators_to_query):
            if attempts > 0:
                time.sleep(delay)
                delay *= 2  # Exponential backoff

            positions = self.query_positions(validators_to_query, hotkey_to_positions)
            for p in positions:
                hotkey_to_positions[p[0].hotkey] = p[1]

            attempts += 1

        bt.logging.info(f"Got positions from {len(hotkey_to_positions)} possible validators")
        # We consider a validator acked if it successfully responded to the signal.
        # Note, a validator that has this miner blacklisted will not be added.
        self.recently_acked_validators = hotkey_to_positions.keys()
        position_most_orders = self.reconcile_validator_positions(hotkey_to_positions, validators_to_query)
        # Return the validator with the most orders
        return position_most_orders

    def refresh_allowed(self):
        return (time.time() - self.last_update_time) > self.UPDATE_INTERVAL_S

    def log_validator_positions(self):
        """
        Sends signals to the validators to get their time-sorted positions for this miner.
        This method may be used directly in your own logic to attempt to "fix" validator positions.
        Note: The rate limiter on validators will prevent repeated calls from succeeding if they are too frequent.
        """
        if not self.refresh_allowed():
            return

        validators_to_query = self.get_possible_validators()
        bt.logging.info(f"Querying {len(validators_to_query)} possible validators for positions")
        result = self.get_positions_with_retry(validators_to_query)

        if not result:
            bt.logging.info("No positions found.")

        self.last_update_time = time.time()
        bt.logging.success("PositionInspector successfully completed signal processing.")


if __name__ == "__main__":
    def get_config():
        parser = argparse.ArgumentParser()
        # Adds override arguments for network and netuid.
        parser.add_argument("--netuid", type=int, default=1, help="The chain subnet uid.")
        # Adds subtensor specific arguments i.e. --subtensor.chain_endpoint ... --subtensor.network ...
        bt.subtensor.add_args(parser)
        # Adds logging specific arguments i.e. --logging.debug ..., --logging.trace .. or --logging.logging_dir ...
        bt.logging.add_args(parser)
        # Adds wallet specific arguments i.e. --wallet.name ..., --wallet.hotkey ./. or --wallet.path ...
        bt.wallet.add_args(parser)
        # Adds an argument to allow setting write_failed_signal_logs from the command line
        # We use a placeholder default value here (None) to check if the user has provided a value later
        parser.add_argument("--write_failed_signal_logs", type=bool, default=None,
                            help="Whether to write logs for failed signals. Default is True unless --subtensor.network is 'test'.")
        parser.add_argument(
            '--start-dashboard',
            action='store_true',
            help='Start the miner-dashboard along with the miner.'
        )

        # Parse the config (will take command-line arguments if provided)
        # To print help message, run python3 template/miner.py --help
        config = bt.config(parser)
        bt.logging.enable_default()
        if config.logging.debug:
            bt.logging.enable_debug()
        if config.logging.trace:
            bt.logging.enable_trace()

        # Determine the default value for write_failed_signal_logs based on the subtensor.network,
        # but only if the user hasn't explicitly set it via command line.
        if config.write_failed_signal_logs is None:
            config.write_failed_signal_logs = False if config.subtensor.network == "test" else True

        # Step 3: Set up logging directory
        # Logging is crucial for monitoring and debugging purposes.
        config.full_path = os.path.expanduser(
            "{}/{}/{}/netuid{}/{}".format(
                config.logging.logging_dir,
                config.wallet.name,
                config.wallet.hotkey,
                config.netuid,
                "miner",
            )
        )
        return config
    config = get_config()
    wallet = bt.wallet(config=config)
    subtensor = bt.subtensor(config=config)
    
    # Setup metagraph
    metagraph = subtensor.metagraph(netuid=config.netuid)
    metagraph.sync()
    position_inspector = PositionInspector(wallet, metagraph, config)
    
