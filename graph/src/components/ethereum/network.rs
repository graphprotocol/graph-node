use alloy::network::{
    AnyHeader, AnyReceiptEnvelope, AnyTxEnvelope, AnyTxType, AnyTypedTransaction, BuildResult,
    Network, NetworkWallet, TransactionBuilder, TransactionBuilderError,
};
use alloy::primitives::{Address, Bytes, ChainId, TxKind, U256};
use alloy::providers::fillers::{
    BlobGasFiller, ChainIdFiller, GasFiller, JoinFill, NonceFiller, RecommendedFillers,
};
use alloy::rpc::types::{
    AccessList, Block, Header, Log, TransactionInputKind, TransactionReceipt, TransactionRequest,
};
use alloy::serde::WithOtherFields;
use std::ops::{Deref, DerefMut};

use super::types::AnyTransaction;

/// Like alloy's [`AnyNetwork`] but without [`WithOtherFields`] on response types.
///
/// `AnyNetwork` wraps every response (block, transaction, receipt) in `WithOtherFields<T>`,
/// which captures unknown JSON keys into a `BTreeMap`. This requires `#[serde(flatten)]`,
/// forcing serde to buffer entire JSON objects into intermediate `Value` maps and
/// re-serialize them to diff keys — on every block and every transaction.
/// graph-node never uses these extra fields, so we define a bare network that deserializes
/// directly into the inner types.
///
/// Uses `AnyTxEnvelope` (not `TxEnvelope`) to support non-standard transaction types from
/// L2s and sidechains. `TransactionRequest` retains `WithOtherFields` because upstream
/// `From` impls for `AnyTxEnvelope` only exist on `WithOtherFields<TransactionRequest>`.
///
/// [`AnyNetwork`]: alloy::network::AnyNetwork
/// [`WithOtherFields`]: alloy::serde::WithOtherFields
#[derive(Clone, Copy, Debug)]
pub struct AnyNetworkBare;

impl Network for AnyNetworkBare {
    type TxType = AnyTxType;
    type TxEnvelope = AnyTxEnvelope;
    type UnsignedTx = AnyTypedTransaction;
    type ReceiptEnvelope = AnyReceiptEnvelope;
    type Header = AnyHeader;

    type TransactionRequest = WithOtherFields<TransactionRequest>;
    type TransactionResponse = AnyTransaction;
    type ReceiptResponse = TransactionReceipt<AnyReceiptEnvelope<Log>>;
    type HeaderResponse = Header<AnyHeader>;
    type BlockResponse = Block<AnyTransaction, Header<AnyHeader>>;
}

impl RecommendedFillers for AnyNetworkBare {
    type RecommendedFillers =
        JoinFill<GasFiller, JoinFill<BlobGasFiller, JoinFill<NonceFiller, ChainIdFiller>>>;

    fn recommended_fillers() -> Self::RecommendedFillers {
        Default::default()
    }
}

impl TransactionBuilder<AnyNetworkBare> for WithOtherFields<TransactionRequest> {
    fn chain_id(&self) -> Option<ChainId> {
        self.deref().chain_id()
    }

    fn set_chain_id(&mut self, chain_id: ChainId) {
        self.deref_mut().set_chain_id(chain_id)
    }

    fn nonce(&self) -> Option<u64> {
        self.deref().nonce()
    }

    fn set_nonce(&mut self, nonce: u64) {
        self.deref_mut().set_nonce(nonce)
    }

    fn take_nonce(&mut self) -> Option<u64> {
        self.deref_mut().nonce.take()
    }

    fn input(&self) -> Option<&Bytes> {
        self.deref().input()
    }

    fn set_input<T: Into<Bytes>>(&mut self, input: T) {
        self.deref_mut().set_input(input);
    }

    fn set_input_kind<T: Into<Bytes>>(&mut self, input: T, kind: TransactionInputKind) {
        self.deref_mut().set_input_kind(input, kind)
    }

    fn from(&self) -> Option<Address> {
        self.deref().from()
    }

    fn set_from(&mut self, from: Address) {
        self.deref_mut().set_from(from);
    }

    fn kind(&self) -> Option<TxKind> {
        self.deref().kind()
    }

    fn clear_kind(&mut self) {
        self.deref_mut().clear_kind()
    }

    fn set_kind(&mut self, kind: TxKind) {
        self.deref_mut().set_kind(kind)
    }

    fn value(&self) -> Option<U256> {
        self.deref().value()
    }

    fn set_value(&mut self, value: U256) {
        self.deref_mut().set_value(value)
    }

    fn gas_price(&self) -> Option<u128> {
        self.deref().gas_price()
    }

    fn set_gas_price(&mut self, gas_price: u128) {
        self.deref_mut().set_gas_price(gas_price);
    }

    fn max_fee_per_gas(&self) -> Option<u128> {
        self.deref().max_fee_per_gas()
    }

    fn set_max_fee_per_gas(&mut self, max_fee_per_gas: u128) {
        self.deref_mut().set_max_fee_per_gas(max_fee_per_gas);
    }

    fn max_priority_fee_per_gas(&self) -> Option<u128> {
        self.deref().max_priority_fee_per_gas()
    }

    fn set_max_priority_fee_per_gas(&mut self, max_priority_fee_per_gas: u128) {
        self.deref_mut()
            .set_max_priority_fee_per_gas(max_priority_fee_per_gas);
    }

    fn gas_limit(&self) -> Option<u64> {
        self.deref().gas_limit()
    }

    fn set_gas_limit(&mut self, gas_limit: u64) {
        self.deref_mut().set_gas_limit(gas_limit);
    }

    fn access_list(&self) -> Option<&AccessList> {
        self.deref().access_list()
    }

    fn set_access_list(&mut self, access_list: AccessList) {
        self.deref_mut().set_access_list(access_list)
    }

    fn complete_type(
        &self,
        ty: <AnyNetworkBare as Network>::TxType,
    ) -> Result<(), Vec<&'static str>> {
        self.deref()
            .complete_type(ty.try_into().map_err(|_| vec!["supported tx type"])?)
    }

    fn can_submit(&self) -> bool {
        self.deref().can_submit()
    }

    fn can_build(&self) -> bool {
        self.deref().can_build()
    }

    fn output_tx_type(&self) -> <AnyNetworkBare as Network>::TxType {
        self.deref().output_tx_type().into()
    }

    fn output_tx_type_checked(&self) -> Option<<AnyNetworkBare as Network>::TxType> {
        self.deref().output_tx_type_checked().map(Into::into)
    }

    fn prep_for_submission(&mut self) {
        self.deref_mut().prep_for_submission()
    }

    fn build_unsigned(
        self,
    ) -> BuildResult<<AnyNetworkBare as Network>::UnsignedTx, AnyNetworkBare> {
        if let Err((tx_type, missing)) = self.missing_keys() {
            return Err(TransactionBuilderError::InvalidTransactionRequest(
                tx_type.into(),
                missing,
            )
            .into_unbuilt(self));
        }
        Ok(self
            .inner
            .build_typed_tx()
            .expect("checked by missing_keys")
            .into())
    }

    async fn build<W: NetworkWallet<AnyNetworkBare>>(
        self,
        wallet: &W,
    ) -> Result<<AnyNetworkBare as Network>::TxEnvelope, TransactionBuilderError<AnyNetworkBare>>
    {
        Ok(wallet.sign_request(self).await?)
    }
}
